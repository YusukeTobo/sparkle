/*
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.hp.hpl.firesteel.shuffle;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;

import scala.Tuple2;
import scala.collection.Iterator;

import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.util.ByteBufferInputStream;
import org.apache.spark.sql.execution.UnsafeRowSerializerInstance;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.ReduceStatus;
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.MergeSortedResult;
import com.hp.hpl.firesteel.shuffle.ShuffleDataModel.KValueTypeId;

public class ReduceSHMShuffleStore implements ReduceShuffleStore {
    private static final Logger LOG =
        LoggerFactory.getLogger(ReduceSHMShuffleStore.class.getName());

    private ByteBuffer byteBuffer = null;
    // NOTE: Avoid `deserialize` methods because they read the whole buffer and set position to its limit.
    //       Alternatively, we need to use deserializeStream to get remaining objects inside the buffer.
    private SerializerInstance serializer = null;
    private ShuffleStoreManager shuffleStoreManager= null;

    private static AtomicInteger storeCounter = new AtomicInteger(0);
    private int storeId;

    public ReduceSHMShuffleStore(Serializer serializer, ByteBuffer byteBuffer, ShuffleStoreManager shuffleStoreManager) {
        if (!byteBuffer.isDirect()) {
            throw new IllegalArgumentException("ByteBuffer must be DirectBuffer.");
        }

        this.serializer = serializer.newInstance();
        this.byteBuffer = byteBuffer;
        this.byteBuffer.clear();

        this.shuffleStoreManager = shuffleStoreManager;
        this.storeId = storeCounter.getAndIncrement();
    }

    private long pointerToStore = 0;
    private int shuffleId = 0;
    private int reduceId = 0;
    private int numberOfPartitions = 0;
    //to keep track of the key ordering property
    private boolean ordering;
    //to keep track of the aggregation property
    private boolean aggregation;

    private boolean enableJniCallback = false;
    public void setEnableJniCallback(boolean doJniCallback) {
        this.enableJniCallback = doJniCallback;
    }
    public boolean getEnableJniCallback() {
        return this.enableJniCallback;
    }

    private int numFetchedKvPairs = 0; // from kvPairBuffer or kvPairMap.
    private Iterator<Tuple2<Object, Object>> kvPairIter = null;
    private List<Tuple2<Comparable, Object>> kvPairBuffer = new ArrayList<>();
    private Comparator<Tuple2<Comparable, Object>> comp = new Comparator<Tuple2<Comparable, Object>>() {
            @Override
            public int compare(Tuple2<Comparable, Object> p1, Tuple2<Comparable, Object> p2) {
                return p1._1.compareTo(p2._1);
            }
        };

    private Map<Object, ArrayList<Object>> kvPairMap = new HashMap<>();
    private Object[] kvPairMapKeys;

    @Override
    public void initialize (int shuffleId, int reduceId, int numberOfPartitions, boolean ordering, boolean aggregation) {
        this.shuffleId = shuffleId;
        this.reduceId= reduceId;
        this.numberOfPartitions = numberOfPartitions;
        //defer its use until merge-sort.
        this.ordering = ordering;
        //defer its use until merge, to decide whether we will have straight-forward pass through
        this.aggregation = aggregation;

        ninitialize(this.shuffleStoreManager.getPointer(), shuffleId, reduceId, numberOfPartitions);
        LOG.info("store id " +  this.storeId + " reduce-side shared-memory based shuffle store started with id:"
                 + this.shuffleId + "-" + this.reduceId);
    }

    //NOTE: the key type and value type definition goes through the shuffle channel in C++. We do not need
    //to pass it here.
    //NOTE sure whether we will create the reduce side shuffle store from Java side or not.
    //but stop and shutdown can be. We need to check out Spark's implementation on who drives this.
    private native void ninitialize(
            long ptrToShuffleManager, int shuffleId, int reduceId, int numberOfPartitions);

    //Note: this call currently can only be invoked after mergesort, as that is when the actual
    //reduce shuffle store gets created.
    @Override
    public void stop() {
        LOG.info("store id " + this.storeId + " reduce-side shared-memory based shuffle store stopped with id:"
                + this.shuffleId + "-" + this.reduceId);

        //to return shuffle related resource back to the tracker.
        //ThreadLocalShuffleResourceHolder holder = new ThreadLocalShuffleResourceHolder();
        //ThreadLocalShuffleResourceHolder.ShuffleResource resource = holder.getResource();
        //if (resource != null) {
           //return it to the shuffle resource tracker
        //  this.shuffleStoreManager.getShuffleResourceTracker().recycleSerializationResource(resource);
        //}
        //else {
        // LOG.info("store id " + this.storeId + " reduce-side shared-memory based shuffle store stopped with id:"
        //    + this.shuffleId + "-" + this.reduceId + " has re-usable shuffle resource == null");
        //}

        //then return the native resources as well.
        if (this.pointerToStore != 0L) {
            nstop(this.pointerToStore);
        }
    }

    //shuffle store manager is the creator of map shuffle manager and reduce shuffle manager
    private native void nstop(long ptrToStore);

    @Override
    public void shutdown (){
        if (this.pointerToStore == 0L) {
            LOG.info("store:" + this.storeId + " has been shutdonw");
            return ;
        }

        LOG.info("store id " + this.storeId + "reduce-side shared-memory based shuffle store shutdown with id:"
                         + this.shuffleId + "-" + this.reduceId);
        nshutdown(this.shuffleStoreManager.getPointer(), this.pointerToStore);
    }

    private native void nshutdown(long ptrToShuffleMgr, long ptrToStore);

    @Override
    public void mergeSort(ReduceStatus statuses) {
        if (LOG.isDebugEnabled()) {
            LOG.debug ("store id " + this.storeId + " reduce-side shared-memory based shuffle store perform merge-sort with id:"
                       + this.shuffleId + "-" + this.reduceId);
            int retrieved_mapIds[] = statuses.getMapIds();
            LOG.debug ("store id " + this.storeId + " in mergeSort, total number of maps is: " + retrieved_mapIds.length);
            for (int i=0; i<retrieved_mapIds.length; i++) {
                LOG.debug ("store id " + this.storeId + " ***in mergeSort " + i + "-th map id is: " + retrieved_mapIds[i]);
            }
            long retrieved_regionIdsOfIndexChunks[] = statuses.getRegionIdsOfIndexChunks();
            for (int i=0; i<retrieved_regionIdsOfIndexChunks.length; i++) {
                LOG.debug ("store id " + this.storeId + " ***in mergeSort " + i + "-th region id is: " + retrieved_regionIdsOfIndexChunks[i]);
            }
            long retrieved_chunkoffsets[] = statuses.getOffsetsOfIndexChunks();
            for (int i=0; i<retrieved_chunkoffsets.length; i++) {
                LOG.debug ("store id " + this.storeId +
                           " ***in mergeSort " + i + "-th chunk offset is: 0x " + retrieved_chunkoffsets[i]);
            }
            long retrieved_sizes[] = statuses.getSizes();
            for (int i=0; i<retrieved_sizes.length; i++) {
                LOG.debug ("store id " + this.storeId + " ***in mergeSort " + i + "-th bucket size is: " + retrieved_sizes[i]);
            }
        }

        int totalBuckets = statuses.getMapIds().length;
        this.pointerToStore = nmergeSort(this.shuffleStoreManager.getPointer(), shuffleId, reduceId,
                                         statuses, totalBuckets, this.numberOfPartitions, this.byteBuffer,
                                         this.byteBuffer.capacity(), this.ordering, this.aggregation);
    }

    // NOTE: This method is dedicated to pairs with the Object type key.
    //       ShuffleStore(Like) contains whole serialized pairs in the DRAM from the GlobalHeap.
    //       ShuffleStoreLike is stateless store which copys all serialized paris into DirectBuffer.
    public void createShuffleStore(ReduceStatus statuses) {
        int totalBuckets = statuses.getMapIds().length;

        if (this.enableJniCallback) {
            this.pointerToStore = ncreateShuffleStore(this.shuffleStoreManager.getPointer(), shuffleId, reduceId,
                                                      statuses, totalBuckets, this.byteBuffer,
                                                      this.byteBuffer.capacity(), this.ordering, this.aggregation);
            return ;
        }

        this.byteBuffer.clear();
        nfromShuffleStoreLike(this.shuffleStoreManager.getPointer(), shuffleId, reduceId,
                              statuses, totalBuckets, this.byteBuffer, this.byteBuffer.capacity());
        // NOTE: Inside the native method, the limit of buffer is not updated so that
        //       we need to update the litmit by ouselves.
        this.byteBuffer.limit((int)(statuses.getBytesDataChunks() + statuses.getBytesRemoteDataChunks()));
    }

    /**
     * @param buffer is to passed in the de-serialization buffer's pointer.
     * @param ordering to specify whether the keys need to be ordered at the reduce shuffle store side.
     * @param aggregation to specify whether the values associated with the key needs to be aggregated.
     * if not, we will choose the pass-through.
     * @return the created reduce shuffle store native pointer
     */
    private native long nmergeSort(long ptrToShuffleManager, int shuffleId, int reduceId,
                                         ReduceStatus statuses, int totalBuckets,
                                         int numberOfPartitions, ByteBuffer buffer, int bufferCapacity,
                                         boolean ordering, boolean aggregation);

    private native long ncreateShuffleStore(long ptrToShuffleManager, int shuffleId, int reduceId,
                                            ReduceStatus statuses,
                                            int totalBuckets, ByteBuffer buffer, int bufferCapacity,
                                            boolean ordering, boolean aggregation);

    // copy whole serialized kv pairs in NV buckets to DirectBuffer.
    private native long nfromShuffleStoreLike(long ptrToShuffleManager, int shuffleId, int reduceId,
                                              ReduceStatus statuses,
                                              int totalBuckets, ByteBuffer buffer, int bufferCapacity);

    /**
     * TODO: we will need to design the shared-memory region data structure, so that we can carry
     * the key/value type definition over from map shuffle store via the shuffle channel, plus this enum
     * category information for key.
     * @return
     */
    @Override
    public KValueTypeId getKValueTypeId() {
       int val = ngetKValueTypeId(this.pointerToStore);

       if (val==KValueTypeId.Int.state){
           return KValueTypeId.Int;
       }
       else if (val == KValueTypeId.Long.state){
            return KValueTypeId.Long;
       }
       else if (val == KValueTypeId.Float.state){
           return KValueTypeId.Float;
       }
       else if (val == KValueTypeId.Double.state){
           return KValueTypeId.Double;
       }
       else if (val ==  KValueTypeId.Object.state){
           return KValueTypeId.Object;
       }
       else if (val == KValueTypeId.Unknown.state) {
           throw new RuntimeException("Unknown key value type encountered");
       }
       else {
           throw new RuntimeException("unsupported key value type encountered");
       }
    }

    /**
     * Current implementation in C++ has the Key adn Value type definition retrieved from the merge-sort channel
     * associated with the map bucket. Thus this getKValueTypeID
     * @return
     */
    private native int ngetKValueTypeId(long ptrToStore);

    @Override
    public int getKVPairs (ArrayList<Object> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers,
                           int[] numRawPairs) {
        if (!this.enableJniCallback) {
            return hashMapDirectBuffer(kvalues, vvalues, knumbers, numRawPairs);
        }

        // prep key holders and value offsets in the direct buffer.
        Object okvalues[] = new Object[knumbers];
        int pvVOffsets[] = new int[knumbers];

        this.byteBuffer.clear();
        int actualKVPairs = nGetKVPairs(this.pointerToStore, okvalues,
                                        this.byteBuffer, this.byteBuffer.capacity(),
                                        pvVOffsets, knumbers);

        int prevPos = 0;
        for (int i=0; i<actualKVPairs; i++) {
            kvalues.set(i, okvalues[i]);

            this.byteBuffer.position(prevPos);
            this.byteBuffer.limit(pvVOffsets[i]);
            Iterator<Object> it =
                this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asIterator();

            ArrayList<Object> holder = new ArrayList<>();
            while (it.hasNext()) {
                holder.add(it.next());
            }
            vvalues.set(i, holder);

            prevPos = this.byteBuffer.limit();
        }

        return actualKVPairs;
    }

    private int hashMapDirectBuffer(ArrayList<Object> kvalues, ArrayList<ArrayList<Object>> vvalues,
                                    int knumbers, int[] numRawPairs) {
        // initialize the Reducer.
        if (this.numFetchedKvPairs == 0) {
            Iterator<Object> it =
                this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asIterator();

            int numPairsRead = 0;
            while (it.hasNext()) {
                Object key = it.next();
                kvPairMap.putIfAbsent(key, new ArrayList<>());
                Object value = it.next();
                kvPairMap.get(key).add(value);
                numPairsRead++;
            }
            numRawPairs[0] = numPairsRead;
            this.byteBuffer.clear();

            kvPairMapKeys = kvPairMap.keySet().toArray();
        }

        int numReadPairs = 0;
        for (int i=numFetchedKvPairs; i<kvPairMapKeys.length; ++i) {
            kvalues.set(numReadPairs, kvPairMapKeys[i]);
            vvalues.set(numReadPairs, kvPairMap.get(kvPairMapKeys[i]));
            numReadPairs++;
            numFetchedKvPairs++;
            if (numReadPairs == knumbers) {
                break;
            }
        }

        // cleanup
        if (numReadPairs < knumbers) {
            kvPairMap.clear();
            numFetchedKvPairs = 0;
        }

        return numReadPairs;
    }

    /**
     * No need to specify the key offsets, as there is always only one key
     * per (k, {vp,1,vp,2...vp,k}}.
     * @param byteBuffer holds the information on values that
     * @param voffsets
     * @param knumbers
     * @return
     */
    private native int nGetKVPairs(long ptrToStore, Object kvalues[], ByteBuffer
                                   byteBuffer, int buffer_capacity,
                                   int voffsets[], int knumbers);

    /**
     * return unordered kv pairs to ShuffleReader's Iterator.
     * @param kvalues
     * @param vvalues
     * @param knumbers
     * @return the number of kv pairs which are deserialized from the global heap.
     */
    public int getSimpleKVPairs (ArrayList<Object> kvalues, ArrayList<Object> vvalues, int knumbers) {
        // prep key holders and value offsets in the direct buffer.
        Object okvalues[] = new Object[knumbers];
        int pvVOffsets[] = new int[knumbers];

        if (!this.enableJniCallback) {
            if (!this.ordering) {
                return readDirectBuffer(kvalues, vvalues, knumbers);
            }
            return mergeSortDirectBuffer(kvalues, vvalues, knumbers);
        }

        this.byteBuffer.clear();
        int actualKVPairs = nGetSimpleKVPairs(this.pointerToStore, okvalues,
                                              this.byteBuffer,
                                              this.byteBuffer.capacity(), pvVOffsets, knumbers);

        Iterator<Object> it =
            this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asIterator();

        for (int i=0; i<actualKVPairs; i++) {
            kvalues.set(i, okvalues[i]);
            vvalues.set(i, it.next());
        }

        return actualKVPairs;
    }

    /**
     * @param knumbers the max number of pairs to be read.
     * @return # of pairs read. It would be less than knumber because of the end of buffer.
     */
    private int readDirectBuffer(ArrayList<Object> kvalues, ArrayList<Object> vvalues, int knumbers) {
        if (kvPairIter == null) {
            kvPairIter = this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asKeyValueIterator();
        }

        int numReadPairs = 0;
        for (int i=0; i<knumbers; ++i) {
            if (!kvPairIter.hasNext()) {
                // cleanup.
                this.byteBuffer.clear();
                kvPairIter = null;
                break;
            }

            Tuple2<Object, Object> kv = kvPairIter.next();
            kvalues.set(i, kv._1);
            vvalues.set(i, kv._2);

            numReadPairs++;
        }

        return numReadPairs;
    }

    private int mergeSortDirectBuffer(ArrayList<Object> kvalues, ArrayList<Object> vvalues, int knumbers) {
        // initialize the Reducer.
        if (this.numFetchedKvPairs == 0) {
            Iterator<Object> it =
                this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asIterator();

            while (it.hasNext()) {
                this.kvPairBuffer.add(new Tuple2<>((Comparable) it.next(), it.next()));
            }

            kvPairBuffer.sort(comp);
            this.byteBuffer.clear();
        }

        int numReadPairs = 0;
        for (int i=numFetchedKvPairs; i<kvPairBuffer.size(); ++i) {
            kvalues.set(numReadPairs, kvPairBuffer.get(i)._1);
            vvalues.set(numReadPairs, kvPairBuffer.get(i)._2);
            numReadPairs++;
            numFetchedKvPairs++;
            if (numReadPairs == knumbers) {
                break;
            }
        }

        // cleanup
        if (numReadPairs < knumbers) {
            kvPairBuffer.clear();
            numFetchedKvPairs = 0;
        }

        return numReadPairs;
    }

    private native int nGetSimpleKVPairs(long ptrToStore, Object kvalues[], ByteBuffer
                                         byteBuffer, int buffer_capacity,
                                         int voffsets[], int knumbers);

    //vvalues only has the first layer of element populated with empty object. the second level
    //of object array will have to be created by Java.
    @Override
    public int getKVPairsWithIntKeys (ArrayList<Integer> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {
      MergeSortedResult mergeResult = new MergeSortedResult();
      boolean bufferExceeded = mergeResult.getBufferExceeded();
      if (bufferExceeded) {
          LOG.error( "store id " + this.storeId +
                     " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
          return 0;
      }

      this.byteBuffer.clear();
      int actualKVPairs = nGetKVPairsWithIntKeys(this.pointerToStore,
              this.byteBuffer, this.byteBuffer.capacity(), knumbers, mergeResult);

      //then populate back to the list the key values.
      int[] pkValues = mergeResult.getIntKvalues();
      int[] pvVOffsets =mergeResult.getVoffsets(); //offset to each group of {vp,1, vp,2...,vp,k}.
      int prevPos = 0;
      for (int i=0; i<actualKVPairs; i++){
          kvalues.set(i, pkValues[i]);

          this.byteBuffer.position(prevPos);
          this.byteBuffer.limit(pvVOffsets[i]);

          Iterator<Object> it =
              this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asIterator();

          ArrayList<Object> holder = new ArrayList<>();
          while (it.hasNext()) {
              holder.add(it.next());
          }

          vvalues.set(i, holder);

          prevPos = this.byteBuffer.limit();
      }

      if (LOG.isDebugEnabled()) {
          LOG.debug ( "store id " + this.storeId +
                      " in method getKVPairsWithIntKeys, actual KV pairs received is: " + actualKVPairs);

          for (int i=0; i<actualKVPairs; i++)  {
              LOG.debug ( i + "-th key: " + kvalues.get(i));
              ArrayList<Object> rvvalues = vvalues.get(i);
              for (int m=0; m < rvvalues.size(); m++) {
                  LOG.debug ( "store id " + this.storeId + " **" + m + "-th value: " + rvvalues.get(m));
              }
          }
      }

      return actualKVPairs;
    }

    /**
     * the native call to pass the keys in an integer array, while having the byte buffer to hold
     * the values {v11,v12, ...v1n} for key k1, which will need to be de-serialized at the Java side
     * byte the Kryo serializer.
     *
     * @param byteBuffer holding the byte buffer that will be passed in for de-serialization. The byte buffer
     *                   is already allocated from the de-serializer side. We just need to copy data into
     *                   this buffer!
     * @param mergeResult the holder that holds the kvalue[] and voffsets[], and maybe the indicator
     *                    that the single buffer is not sufficient to hold the values for the number of
     *                    key-values retrieved.
     * @param knumbers the specified maximum number of keys retrieved
     * @return the actual number of keys retrieved
     */
    private native int nGetKVPairsWithIntKeys(long ptrToShuffleStore,
                      ByteBuffer byteBuffer, int buffer_capacity,
                      int knumbers, MergeSortedResult mergeResult);

    public boolean isUnsafeRow() {
        return this.serializer instanceof UnsafeRowSerializerInstance;
    }

    public Iterator<Tuple2<Object, Object>> getSimpleKVPairsWithIntKeys() {
        MergeSortedResult mergeResult = new MergeSortedResult();
        boolean bufferExceeded = mergeResult.getBufferExceeded();
        if (bufferExceeded) {
            LOG.error("store id " + this.storeId +
                      " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
            return null;
        }

        this.byteBuffer.clear();
        int actualKVPairs =
            nGetSimpleKVPairsWithIntKeys(this.pointerToStore, this.byteBuffer,
                                         this.byteBuffer.capacity(), Integer.MAX_VALUE, mergeResult);

        LOG.info(String.format("reducerId[%d]: %d pairs fetched"
                               , reduceId, actualKVPairs));

        if (actualKVPairs == 0) {
            return null;
        }

        int[] pvVOffsets =mergeResult.getVoffsets();
        this.byteBuffer.limit(pvVOffsets[actualKVPairs -1]);

        return this.serializer
            .deserializeStream(new ByteBufferInputStream(this.byteBuffer))
            .asKeyValueIterator();
    }

    @Override
    public  int getSimpleKVPairsWithIntKeys (ArrayList<Integer>kvalues, ArrayList<Object> values, int knumbers) {
        //I can still use the same APIs for the simple key/value pairs retrieval
        MergeSortedResult mergeResult = new MergeSortedResult();
        boolean bufferExceeded = mergeResult.getBufferExceeded();
        if (bufferExceeded) {
            LOG.error("store id " + this.storeId +
                      " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
            return 0;
        }

        this.byteBuffer.clear();
        int actualKVPairs =
            nGetSimpleKVPairsWithIntKeys(this.pointerToStore, this.byteBuffer,
                                         this.byteBuffer.capacity(), knumbers, mergeResult);

        if (actualKVPairs == 0) {
            return 0;
        }

        //then populate back to the list the key values.
        int[] pkValues = mergeResult.getIntKvalues();
        int[] pvVOffsets =mergeResult.getVoffsets();
        this.byteBuffer.limit(pvVOffsets[actualKVPairs-1]);
        Iterator<Object> it =
            this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asIterator();
        for (int i=0; i<actualKVPairs; i++){
            kvalues.set(i, pkValues[i]);
            values.set(i, it.next());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug ( "store id " + this.storeId +
                        " in method getSimpleKVPairsWithIntKeys, actual KV pairs received is: " + actualKVPairs);

            for (int i=0; i<actualKVPairs; i++)  {
                LOG.debug ( i + "-th key: " + kvalues.get(i));
                Object rvvalue = values.get(i);
                LOG.debug ( "store id " + this.storeId + " ** retrieved value: " + rvvalue);
            }
        }

        return actualKVPairs;
    }

    private native int nGetSimpleKVPairsWithIntKeys(long ptrToShuffleStore,
            ByteBuffer byteBuffer, int buffer_capacity,
            int knumbers, MergeSortedResult mergeResult);

    @Deprecated
    @Override
    public int getKVPairsWithFloatKeys (ArrayList<Float> kvalues, ArrayList<ArrayList<Object>> vvalues, int knumbers) {
        MergeSortedResult mergeResult = new MergeSortedResult();

        this.byteBuffer.clear();
        int actualKVPairs =
            nGetKVPairsWithFloatKeys(this.pointerToStore, this.byteBuffer,
                                     this.byteBuffer.capacity(), knumbers, mergeResult);

        float pkValues[] = mergeResult.getFloatKvalues();
        int pvVOffsets[] = mergeResult.getVoffsets(); //offset to each group of {vp,1, vp,2...,vp,k}.

        //then populate back to the list the key values.
        for (int i=0; i<actualKVPairs; i++){
            kvalues.set(i, pkValues[i]);
            //the corresponding value pairs
            ArrayList<Object> holder = new ArrayList<Object>();

            {
                int endPosition = pvVOffsets[i];
                while (this.byteBuffer.position() < endPosition){
                    //Object p = this.serializer.deserialize(this.byteBuffer, OBJ_CLASS_TAG);
                    //holder.add(p);
                }
            }

            vvalues.set(i, holder);
        }

        return actualKVPairs;
    }

    private native int nGetKVPairsWithFloatKeys(long ptrToShuffleStore,
                        ByteBuffer byteBuffer, int buffer_capacity,
                        int knumbers, MergeSortedResult mergeResult);

    @Deprecated
    @Override
    public int getSimpleKVPairsWithFloatKeys (ArrayList<Float> kvalues, ArrayList<Object> values, int knumbers) {
        MergeSortedResult mergeResult = new MergeSortedResult();

        this.byteBuffer.clear();
        int actualKVPairs =
            nGetSimpleKVPairsWithFloatKeys(this.pointerToStore, this.byteBuffer,
                                           this.byteBuffer.capacity(), knumbers, mergeResult);

        float pkValues[] = mergeResult.getFloatKvalues();
        int pvVOffsets[] = mergeResult.getVoffsets();  //offset to each group of {vp,1, vp,2...,vp,k}.

        //then populate back to the list the key values.
        for (int i=0; i<actualKVPairs; i++){
            kvalues.set(i, pkValues[i]);
            Object p = null;
            {
                int endPosition = pvVOffsets[i];
                if (this.byteBuffer.position() < endPosition){
                    //p = this.serializer.deserialize(this.byteBuffer, OBJ_CLASS_TAG);
                }
                else {
                    throw new RuntimeException ("deserializer cannot de-serialize object following the offset boundary");
                }
            }

            values.set(i, p);
        }

        return actualKVPairs;
    }

    private native int nGetSimpleKVPairsWithFloatKeys(long ptrToShuffleStore,
            ByteBuffer byteBuffer, int buffer_capacity,
            int knumbers, MergeSortedResult mergeResult);

    @Override
    public int getKVPairsWithLongKeys (ArrayList<Long> kvalues,  ArrayList<ArrayList<Object>> vvalues, int knumbers){
        MergeSortedResult mergeResult = new MergeSortedResult();
        boolean bufferExceeded = mergeResult.getBufferExceeded();
        if (bufferExceeded) {
            LOG.error("store id " + this.storeId +
                      " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
            return 0;
        }

        this.byteBuffer.clear();
        int actualKVPairs =
            nGetKVPairsWithLongKeys(this.pointerToStore, this.byteBuffer,
                                    this.byteBuffer.capacity(), knumbers, mergeResult);

        long pkValues[] = mergeResult.getLongKvalues();
        int pvVOffsets[] = mergeResult.getVoffsets();  //offset to each group of {vp,1, vp,2...,vp,k}.
        //then populate back to the list the key values.
        int prevPos = 0;
        for (int i=0; i<actualKVPairs; ++i){
            kvalues.set(i, pkValues[i]);

            this.byteBuffer.position(prevPos);
            this.byteBuffer.limit(pvVOffsets[i]);
            // NOTE: deserializeStream is really slow method, but I can only use it from SerializeInstance...
            scala.collection.Iterator<Object> it =
                this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asIterator();

            ArrayList<Object> holder = new ArrayList<>();
            while (it.hasNext()) {
                holder.add(it.next());
            }
            vvalues.set(i, holder);

            prevPos = this.byteBuffer.limit();
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug ( "store id " + this.storeId +
                        " in method getKVPairsWithLongKeys, actual KV pairs received is: " + actualKVPairs);

            for (int i=0; i<actualKVPairs; i++)  {
                LOG.debug ( i + "-th key: " + kvalues.get(i));
                ArrayList<Object> rvvalues = vvalues.get(i);
                for (int m=0; m < rvvalues.size(); m++) {
                    LOG.debug ( "store id " + this.storeId + " **" + m + "-th value: " + rvvalues.get(m));
                }
            }
        }

        return actualKVPairs;
    }

    private native int nGetKVPairsWithLongKeys(long pointerToStore,
                       ByteBuffer byteBuffer, int buffer_capacity,
                       int knumbers, MergeSortedResult mergeResult);

    @Override
    public int getSimpleKVPairsWithLongKeys (ArrayList<Long> kvalues, ArrayList<Object> values, int knumbers) {
        MergeSortedResult mergeResult = new MergeSortedResult();
        boolean bufferExceeded = mergeResult.getBufferExceeded();
        if (bufferExceeded) {
            LOG.error("store id " + this.storeId +
                      " deserialization buffer for shm shuffle reducer is exceeded; need to configure a bigger one");
            return 0;
        }

        this.byteBuffer.clear();
        int actualKVPairs =
            nGetSimpleKVPairsWithLongKeys(this.pointerToStore, this.byteBuffer,
                                          this.byteBuffer.capacity(), knumbers, mergeResult);

        if (actualKVPairs == 0) {
            return 0;
        }

        long pkValues[] = mergeResult.getLongKvalues();
        int pvVOffsets[] = mergeResult.getVoffsets();

        this.byteBuffer.limit(pvVOffsets[actualKVPairs-1]);
        Iterator<Object> it =
            this.serializer.deserializeStream(new ByteBufferInputStream(this.byteBuffer)).asIterator();
        for (int i=0; i<actualKVPairs; i++){
            kvalues.set(i, pkValues[i]);
            values.set(i, it.next());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug ( "store id " + this.storeId +
                        " in method getSimpleKVPairsWithLongKeys, actual KV pairs received is: " + actualKVPairs);
            for (int i=0; i<actualKVPairs; i++)  {
                LOG.debug ( i + "-th key: " + kvalues.get(i));
                Object rvvalue = values.get(i);
                LOG.debug ( "store id " + this.storeId + " ** retrieved value: " + rvvalue);
            }
        }

        return actualKVPairs;
    }

    private native int nGetSimpleKVPairsWithLongKeys(long pointerToStore,
            ByteBuffer byteBuffer, int buffer_capacity,
            int knumbers, MergeSortedResult mergeResult);

    @Override
    public int getStoreId() {
        return this.storeId;
    }
}

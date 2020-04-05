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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * to implement the Mapside Shuffle Store. We will use the Kryo serializer to do internal
 * object serialization/de-serialization
 */
public class MapSHMShuffleStore implements MapShuffleStore {

    private static final Logger LOG = LoggerFactory.getLogger(MapSHMShuffleStore.class.getName());

    private SerializerInstance serializer = null;
    private ByteBuffer byteBuffer = null;

    private ShuffleStoreManager shuffleStoreManager=null;

    private static AtomicInteger storeCounter = new AtomicInteger(0);
    private int storeId;
    //serializer creates serialization instance. for a long lived executor, the thread pool can
    //reuse per-thread serialization instance

    /**
     * @param serializer serializer for kv pairs.
     * @param byteBuffer needs to have the reusable bytebuffer from a  re-usable pool as well
     */
    public  MapSHMShuffleStore(Serializer serializer, ByteBuffer byteBuffer,ShuffleStoreManager shuffleStoreManager) {
        if (!byteBuffer.isDirect()) {
            throw new IllegalArgumentException("ByteBuffer must be DirectBuffer.");
        }

        this.serializer = serializer.newInstance();
        this.byteBuffer = byteBuffer;
        this.byteBuffer.clear();

        this.shuffleStoreManager= shuffleStoreManager;
        this.storeId = storeCounter.getAndIncrement();
    }

    private long pointerToStore=0;
    private int shuffleId=0;
    private int mapTaskId = 0;
    private int numberOfPartitions =0;
    private ShuffleDataModel.KValueTypeId keyType;

    //record the size of the pre-defined batch serialization size
    private int sizeOfBatchSerialization=0;

    //the following two sets of array declaration is such that in one map shuffle whole duration,
    //we can pre-allocate the data structure required once, and then keep re-use for each iteration of
    //store key/value pairs to C++ shuffle engine
    private int koffsets[] = null;
    private int voffsets[]  =null;
    private int npartitions[] = null;

    //the following targets different key/value pair, not all of the defined structures will be activated
    //as it depends on which key type the map store is to handle.
    private int nkvalues[] = null;
    private float fkvalues[] = null;
    private long  lkvalues[]  = null;
    private Object okvalues[] = null;
    private int okhashes[] = null;

    //add key ordering specification for the map/reduce shuffle store
    private boolean ordering;

    // enable JNI callbacks by the flag.
    private boolean enableJniCallback = false;
    public void setEnableJniCallback(boolean doJniCallback) {
        this.enableJniCallback = doJniCallback;
        LOG.info("Jni Callback: " + this.enableJniCallback);
    }

    /**
     * to initialize the storage space for a particular shuffle stage's map instance
     * @param shuffleId the current stage id
     * @param mapTaskId  the current map task Id
     * @param numberOfPartitions  the total number of the partitions chosen for the reducer.
     * @param keyType the type of the key, so that we can create the corresponding one in C++.
     * @param batchSerialization, the size of the predefined serialization batch
     * @param odering, to specify whether the keys need to be ordered or not, for the map shuffle.
     * @return sessionId, which is the pointer to the native C++ shuffle store.
     */
     @Override
     public void initialize(int shuffleId, int mapTaskId, int numberOfPartitions,
                                 ShuffleDataModel.KValueTypeId keyType, int batchSerialization,
                                 boolean ordering){

         LOG.info("store id" + this.storeId
                  + " map-side shared-memory based shuffle store started"
                  + " with ordering: " + ordering);

         this.shuffleId = shuffleId;
         this.mapTaskId= mapTaskId;
         this.numberOfPartitions = numberOfPartitions;
         this.keyType = keyType;
         this.sizeOfBatchSerialization =  batchSerialization;
         this.ordering = ordering;

         this.koffsets = new int[this.sizeOfBatchSerialization];
         this.voffsets = new int[this.sizeOfBatchSerialization];
         this.npartitions = new int[this.sizeOfBatchSerialization];

         if (keyType == ShuffleDataModel.KValueTypeId.Int) {
             this.nkvalues = new int[this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Float) {
            this.fkvalues = new float [this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Long) {
             this.lkvalues = new long [this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Double) {
             this.lkvalues = new long [this.sizeOfBatchSerialization];
         }
         else if (keyType == ShuffleDataModel.KValueTypeId.Object){
             this.okvalues = new Object[this.sizeOfBatchSerialization];
             this.okhashes = new int[this.sizeOfBatchSerialization];
         }
         else {
            throw new UnsupportedOperationException ( "key type: " + keyType + " is not supported");
         }

         this.pointerToStore=
             ninitialize(this.shuffleStoreManager.getPointer(),
                         shuffleId, mapTaskId, numberOfPartitions,
                         keyType.getState(), ordering);
     }

     private native long ninitialize(
             long ptrToShuffleManager, int shuffleId, int mapTaskId, int numberOfPartitions,
             int keyType, boolean ordering);


    @Override
    public void stop() {
        LOG.info( "store id " + this.storeId + " map-side shared-memory based shuffle store stopped with id:"
                + this.shuffleId + "-" + this.mapTaskId);
        //recycle the shuffle resource
        //(1) retrieve the shuffle resource object from the thread specific storage
        //(2) return it to the shuffle resource tracker
        //ThreadLocalShuffleResourceHolder holder = new ThreadLocalShuffleResourceHolder();
        //ThreadLocalShuffleResourceHolder.ShuffleResource resource = holder.getResource();
        //if (resource != null) {
           //return it to the shuffle resource tracker
        //   this.shuffleStoreManager.getShuffleResourceTracker().recycleSerializationResource(resource);
        //}
        //else {
        //    LOG.error( "store id " + this.storeId + " map-side shared-memory based shuffle store stopped with id:"
        //       + this.shuffleId + "-" + this.mapTaskId + " does not have recycle serialized resource");
        //
        //}
        //then stop the native resources as well.
        nstop(this.pointerToStore);
    }

    //to stop and reclaim the DRAM resource.
    private native void nstop (long ptrToStore);

    /**
     * to shutdown the session and reclaim the NVM resources required for shuffling this map task.
     *
     * NOTE: who will issue this shutdown at what time
     */
    @Override
    public void shutdown() {
        LOG.info( "store id " + this.storeId + " map-side shared-memory based shuffle store shutdown with id:"
                    + this.shuffleId + "-" + this.mapTaskId);
        nshutdown(this.pointerToStore);
    }

    private native void nshutdown(long ptrToStore);

    protected  void serializeVInt (int kvalue, Object vvalue, int partitionId, int indexPosition) {
        this.nkvalues[indexPosition] = kvalue;
        this.npartitions[indexPosition] = partitionId;
        final ClassTag classTag = ClassTag$.MODULE$.apply(Object.class);
        this.byteBuffer.put(this.serializer.serialize(vvalue, classTag));
        this.voffsets[indexPosition]= this.byteBuffer.position();
    }
    protected void serializeVFloat (float kvalue, Object vvalue, int partitionId, int indexPosition) {
        this.fkvalues[indexPosition] = kvalue;
        this.npartitions[indexPosition] = partitionId;
        final ClassTag classTag = ClassTag$.MODULE$.apply(Object.class);
        this.byteBuffer.put(this.serializer.serialize(vvalue, classTag));
        this.voffsets[indexPosition]= this.byteBuffer.position();
    }
    protected void serializeVLong (long kvalue, Object vvalue, int partitionId, int indexPosition) {
        this.lkvalues[indexPosition] = kvalue;
        this.npartitions[indexPosition] = partitionId;
        final ClassTag classTag = ClassTag$.MODULE$.apply(Object.class);
        this.byteBuffer.put(this.serializer.serialize(vvalue, classTag));
        this.voffsets[indexPosition]= this.byteBuffer.position();
    }
    protected void serializeVDouble (double kvalue, Object vvalue, int partitionId, int indexPosition) {
        throw new RuntimeException ("serialize V for double value is not implemented");
    }
    public void serializeVObject (Object kvalue, Object vvalue, int partitionId, int indexPosition) {
        this.okvalues[indexPosition] = kvalue;
        this.okhashes[indexPosition] = kvalue.hashCode();
        this.npartitions[indexPosition] = partitionId;

        // TODO: explain why classTag required.
        final ClassTag classTag = ClassTag$.MODULE$.apply(Object.class);
        this.byteBuffer.put(this.serializer.serialize(vvalue, classTag));
        this.voffsets[indexPosition]= this.byteBuffer.position();
    }

    @Override
    public void serializeKVPair(Object kvalue, Object vvalue, int partitionId, int indexPosition, int scode) {
        //rely on Java to generate fast switch statement.
        switch (scode) {
        case 0:
            serializeVInt (((Integer)kvalue).intValue(), vvalue, partitionId,  indexPosition);
            break;
        case 1:
            serializeVLong (((Long)kvalue).longValue(), vvalue, partitionId,  indexPosition);
            break;
        case 2:
            serializeVFloat (((Float)kvalue).floatValue(), vvalue,  partitionId,  indexPosition);
            break;
        case 3:
            serializeVDouble (((Double)kvalue).doubleValue(), vvalue,  partitionId,  indexPosition);
            break;
        case 6:
            serializeVObject (kvalue, vvalue,  partitionId,  indexPosition);
            break;
        default:
            throw new RuntimeException ("no specialied key-value type expected");
        }
    }

    @Override
    public void storeKVPairs(int numberOfPairs, int scode) {
        switch (scode) {
        case 0:
            storeKVPairsWithIntKeys (numberOfPairs);
            break;
        case 1:
            storeKVPairsWithLongKeys (numberOfPairs);
            break;
        case 2:
            throw new RuntimeException ("key type of float is not implemented");
        case 3:
            throw new RuntimeException ("key type of double is not implemented");
        case 6:
            // If JNI map/reduce disabled,
            // we store serialized records later.
            if (this.enableJniCallback) {
                copyToNativeStore(numberOfPairs);
            }
            break;
        default:
            throw new RuntimeException ("unknown key type is encountered");
        }
    }

    /**
     * Copy arbitrary key-value pairs to the native map-side store.
     * @param numPairs The number of pairs to be transfer to the native store.
     */
    private void copyToNativeStore(int numPairs) {
        this.byteBuffer.flip();

        nCopyToNativeStore(this.pointerToStore, this.byteBuffer,
                           this.voffsets, this.okvalues,
                           this.okhashes, this.npartitions, numPairs);

        this.byteBuffer.clear();
    }

    private native void nCopyToNativeStore (long ptrToStore, ByteBuffer holder, int[] voffsets,
                                            Object[] okvalues, int[] okhashes, int[] partitions, int numPairs);

    //Special case: to store the (K,V) pairs that have the K values to be with type of Integer
    public void storeKVPairsWithIntKeys (int numberOfPairs) {
        this.byteBuffer.flip();
        if (LOG.isDebugEnabled()) {
            LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithIntKeys" + " numberOfPairs is: " + numberOfPairs);
            for (int i=0; i<numberOfPairs; i++) {
                LOG.debug ( "store id " + this.storeId + " " + i + "-th key's value: " + nkvalues[i]);

                int vStart=0;
                if (i>0) {
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;

                LOG.debug ("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug ("store id " + this.storeId + " [artition Id: " + i + " is: "  + npartitions[i]);
            }
        }

        nstoreKVPairsWithIntKeys(this.pointerToStore,
                                 this.byteBuffer, this.voffsets,
                                 this.nkvalues, npartitions,
                                 numberOfPairs);

        this.byteBuffer.clear();
    }

    private native void nstoreKVPairsWithIntKeys(long ptrToStore, ByteBuffer holder, int[] voffsets,
                                                 int [] kvalues, int[] partitions, int numberOfPairs);

    //Special case: to store the (K,V) pairs that have the K values to be with type of float
    @Deprecated
    public void storeKVPairsWithFloatKeys (int numberOfPairs) {
        /*
    	this.serializer.init();//to initialize the serializer;
        ByteBuffer holder = this.serializer.getByteBuffer();
    
        if (LOG.isDebugEnabled()) {
            // parse the values and turn them into bytes.
       	    LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithFloatKeys" + " numberOfPairs is: " + numberOfPairs);
        	
            for (int i=0; i<numberOfPairs;i++) {

                LOG.debug( "store id " + this.storeId + " key: " + i  + "with value: " + fkvalues[i]);

                int vStart=0;
                if (i>0){
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;
                
                LOG.debug("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug("store id " + this.storeId + " partition Id: " + i + " is: "  + npartitions[i]);
            }

        }

        nstoreKVPairsWithFloatKeys(this.pointerToStore, holder, this.voffsets, this.fkvalues, this.npartitions, numberOfPairs);
        this.serializer.init();//to initialize the serializer;
        */
    }

    @Deprecated
    private native void nstoreKVPairsWithFloatKeys (long ptrToStrore, ByteBuffer holder, int[] voffsets,
                                           float[] kvalues, int[] partitions, int numberOfPairs);

    public void storeKVPairsWithLongKeys (int numberOfPairs) {
        this.byteBuffer.flip();

        if (LOG.isDebugEnabled()) {
            LOG.debug ( "store id " + this.storeId + " in method storeKVPairsWithLongKeys" + " numberOfPairs is: " + numberOfPairs);

            // parse the values and turn them into bytes.
            for (int i=0; i<numberOfPairs;i++) {
                LOG.debug( "store id " + this.storeId + " key: " + i  + "with value: " + lkvalues[i]);

                int vStart=0;
                if (i>0) {
                    vStart = voffsets[i-1];
                }
                int vEnd=voffsets[i];
                int vLength = vEnd-vStart;

                LOG.debug("store id " + this.storeId + " value: " + i +  " has length: " + vLength + " start: " + vStart + " end: " + vEnd);
                LOG.debug("store id " + this.storeId + " partition Id: " + i + " is: "  + npartitions[i]);
            }
        }

        nstoreKVPairsWithLongKeys (this.pointerToStore, this.byteBuffer, this.voffsets, this.lkvalues, this.npartitions, numberOfPairs);

        this.byteBuffer.clear();
    }

    private native void nstoreKVPairsWithLongKeys (long ptrToStore, ByteBuffer holder, int[] voffsets,
                                          long[] kvalues, int[] partitions, int numberOfPairs);

    /**
     * to sort and store the sorted data into non-volatile memory that is ready for  the reduder
     * to fetch
     * @return status information that represents the map processing status
     */
    @Override
    public ShuffleDataModel.MapStatus sortAndStore() {
         ShuffleDataModel.MapStatus status =
             new ShuffleDataModel.MapStatus ();

         //the status details will be updated in JNI.
         nsortAndStore(this.pointerToStore, this.numberOfPartitions, status);

         if (LOG.isDebugEnabled()) {
             long retrieved_mapStauts[] = status.getMapStatus();
             long retrieved_shmRegionIdOfIndexChunk = status.getRegionIdOfIndexBucket();
             long retrieved_offsetToIndexBucket = status.getOffsetOfIndexBucket();

             LOG.debug ("store id " + this.storeId +
                        " in sortAndStore, total number of buckets is: " + retrieved_mapStauts.length);
             for (int i=0; i<retrieved_mapStauts.length; i++) {
                 LOG.debug ("store id" + this.storeId + " **in sortAndStore " + i + "-th bucket size: " + retrieved_mapStauts[i]);
             }
             LOG.debug ("store id " + this.storeId +
                        " **in sortAndStore, retrieved shm region name: " + retrieved_shmRegionIdOfIndexChunk);
             LOG.debug ("store id " + this.storeId +
                        " **in sortAndStore, retrieved offset to index chunk is:" + retrieved_offsetToIndexBucket);
         }

         return status;
    }

    /**
     * The map status only returns for each map's bucket, what each reducer will get what size of
     * byte-oriented content.
     *
     * @param ptrToStoreMgr: pointer to store manager.
     * @param mapStatus the array that will be populated by the underly C++ shuffle engine, before
     *                  it gets returned.
     * @return the Map status information on all of the buckets produced from the Map task.
     */
    private native void nsortAndStore (long ptrToStore,
                                       int totalNumberOfPartitions,
                                       ShuffleDataModel.MapStatus mapStatus);

    /**
     * Write partitioned and sorted records into the GlobalHeap.
     * The records must to be serialized and contained in DirectBuffer to use in JNI.
     **/
    public ShuffleDataModel.MapStatus writeToHeap(ByteBuffer buff, int[] sizes) {
        ShuffleDataModel.MapStatus status = new ShuffleDataModel.MapStatus();
        nwriteToHeap(this.pointerToStore, this.numberOfPartitions, sizes, buff, status);
        return status;
    }

    private native void nwriteToHeap(long ptrToStore,
                                     int totalNumberOfPartitions,
                                     int[] partitionLengths,
                                     ByteBuffer holder,
                                     ShuffleDataModel.MapStatus mapStatus);

    @Override
    public int getStoreId() {
        return this.storeId;
    }
}

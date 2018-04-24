package com.ms.silverking.cloud.dht.client.impl;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.AsyncInvalidation;
import com.ms.silverking.cloud.dht.client.AsyncPut;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.VersionProvider;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.MetaDataConstants;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.OpResultListener;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutMessageGroup.ValueAdditionResult;
import com.ms.silverking.compression.CodecProvider;
import com.ms.silverking.compression.Compressor;
import com.ms.silverking.log.Log;
import com.ms.silverking.util.ArrayUtil;

/**
 * An active PutOperation
 *
 * @param <K> key type
 * @param <V> value type
 */
class AsyncPutOperationImpl<K,V> extends AsyncKVOperationImpl<K,V> 
        implements AsyncPut<K>, OpResultListener, ActiveKeyedOperationResultListener<OpResult>, AsyncInvalidation<K> {
    private final PutOperation<K, V>   putOperation;
	private final long                 version;
    private final AtomicLong           resolvedVersion;
    private final VersionProvider		versionProvider;
	// FUTURE - think about whether we want the double map
	// is the overhead worth the savings in crypto op reduction?
    //protected final ConcurrentMap<K,OpResult>  results; 
    private final ConcurrentMap<DHTKey,OpResult>   opResults;
    private final ActivePutListeners    activePutListeners;
    
    private final List<OperationUUID>   opUUIDs; // holds references to ops to prevent GC
    private List<SegmentedPutValue>     segmentedPutValues; // hold references to prevent GC
    	
	private static final boolean   debug = false;
    private static final boolean   verboseToString = true;
	
	AsyncPutOperationImpl(PutOperation<K,V> putOperation, 
	                          ClientNamespace namespace,
	                          NamespacePerspectiveOptionsImpl<K,V> nspoImpl,
							  long curTimeMillis,
							  byte[] originator,
							  VersionProvider versionProvider) { 
		super(putOperation, namespace, nspoImpl, curTimeMillis, originator);
		this.putOperation = putOperation;
		if (putOperation.size() == 0) {
            setResult(OpResult.SUCCEEDED);
		}
		this.version = putOperation.putOptions().getVersion();
		resolvedVersion = new AtomicLong(DHTConstants.noSuchVersion);
		this.versionProvider = versionProvider;
		this.opResults = new ConcurrentHashMap<>();
		this.activePutListeners = namespace.getActivePutListeners();
		opUUIDs = new LinkedList<>();
		
		//Log.warning(namespace.getOptions().getVersionMode() +" "+ versionProvider 
		//		+" "+ (versionProvider != null ? versionProvider.getVersion() : ""));
	}
	
    @Override 
	protected NonExistenceResponse getNonExistenceResponse() {
    	return null;
    }
	
	@Override
    public MessageEstimate createMessageEstimate() {
        return new PutMessageEstimate();
    }
	
	@Override
    ProtoMessageGroup createProtoMG(MessageEstimate estimate) {
        return createProtoPutMG((PutMessageEstimate)estimate, DHTClient.getValueCreator().getBytes());
    }
	
	PutOptions putOptions() {
	    return putOperation.putOptions();
	}
	
	//private void ensureResolved() {
	//    if (resolvedVersion.get() == 0) {
	//        throw new RuntimeException("Unresolved");
	//    }
	//}
	
    long getPotentiallyUnresolvedVersion() {
        return version;
    }
    
	long getResolvedVersion() {
	    resolveVersion();
	    return resolvedVersion.get();
	}
	
	/**
	 * We delay resolving the version to improve the accuracy for fine-grained version providers.
	 */
	private void resolveVersion() {
	    if (resolvedVersion.get() == DHTConstants.noSuchVersion) {
	    	long	v;
	    	
	    	v = putOperation.putOptions().getVersion();
	    	if (v == PutOptions.defaultVersion) {
	    		v = versionProvider.getVersion();
	    	}
            resolvedVersion.compareAndSet(DHTConstants.noSuchVersion, v);
	    }
	}
    
    ProtoPutMessageGroup<V> createProtoPutMG(PutMessageEstimate estimate) {
        return createProtoPutMG(estimate, DHTClient.getValueCreator().getBytes());
    }
    
	ProtoPutMessageGroup<V> createProtoPutMG(PutMessageEstimate estimate, byte[] creator) {
	    OperationUUID  opUUID;
	    ConcurrentMap<DHTKey,ActiveKeyedOperationResultListener<OpResult>>  newMap;
	    long	resolvedVersion;
	    
	    resolvedVersion = getResolvedVersion();
	    opUUID = activePutListeners.newOpUUIDAndMap();
        return new ProtoPutMessageGroup<>(opUUID, 
                                          context.contextAsLong(),
                                          estimate.getNumKeys(),
                                          estimate.getNumBytes(),
                                          resolvedVersion,
                                          nspoImpl.getValueSerializer(),
                                          putOperation.putOptions().version(resolvedVersion), 
                                          putOperation.putOptions().getChecksumType(), 
                                          originator,
                                          creator,
                                          operation.getTimeoutController().getMaxRelativeTimeoutMillis(this), 
                                          nspoImpl.getNSPOptions().getEncrypterDecrypter());
                                          // FUTURE - trim the above timeout according to the amount
                                          // of time that has elapsed since the start
	}
	
	//private PutMessageEstimate initialEstimate;
	
	@Override
    public void addToEstimate(MessageEstimate estimate) {
	    PutMessageEstimate putMessageEstimate;
	    
	    putMessageEstimate = (PutMessageEstimate)estimate;
	    putMessageEstimate.addKeys(putOperation.size());
        for (K key : getKeys()) {            
            if (!getSent() || OpResult.isIncompleteOrNull(opResults.get(keyToDHTKey.get(key)))) {
                V   value;
                int estimatedValueSize;
                
                value = putOperation.getValue(key);
                estimatedValueSize = nspoImpl.getValueSerializer().estimateSerializedSize(value);
                if (estimatedValueSize > SegmentationUtil.maxValueSegmentSize) {
                    // FIXME - take action
                }
                putMessageEstimate.addBytes(estimatedValueSize);
            }
        }
    }
	
	@Override
    ProtoMessageGroup createMessagesForIncomplete(ProtoMessageGroup protoMG, List<MessageGroup> messageGroups,
                                        MessageEstimate estimate) {
        return createMessagesForIncomplete((ProtoPutMessageGroup<V>)protoMG, messageGroups, (PutMessageEstimate)estimate);
    }
	
	private int    creationCalls;
    
    private ProtoPutMessageGroup<V> createMessagesForIncomplete(ProtoPutMessageGroup<V> protoPutMG, List<MessageGroup> messageGroups, 
            PutMessageEstimate estimate) {
        int oldSegmentsCreated;
        // FUTURE - MAKE SURE THAT THERE IS SOME WORK TO BE DONE
        // now fill in keys and values
        
        // FUTURE - we would like to iterate on DHTKeys to avoid the keyToDHTKey lookup
        // except that we need to retrieve values by the user key.
        
        //if (estimate.getNumKeys() == 0) {
        //    estimate.re.printStackTrace();
        //}
        assert estimate.getNumKeys() != 0;
        
        oldSegmentsCreated = segmentsCreated;
        creationCalls++;

        resolveVersion();
        
        if (debug) {
            System.out.printf("createMessagesForIncomplete() %d\n", getKeys().size());
        }
        
        for (K key : getKeys()) {            
            if (debug) {
                Log.warning(String.format("getSent() %s opResults.get(keyToDHTKey.get(key)) %s\n", 
                        getSent(), opResults.get(keyToDHTKey.get(key))));
            }
            if (!getSent() || OpResult.isIncompleteOrNull(opResults.get(keyToDHTKey.get(key)))) {
                DHTKey  dhtKey;
                V       value;
                ValueAdditionResult additionResult;
                boolean listenerInserted;
                
                dhtKey = keyToDHTKey.get(key);
                value = putOperation.getValue(key);
                
                listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), dhtKey, this);
                if (listenerInserted) {
                    if (debug) {
                        Log.warning(String.format("add1\t"+ dhtKey +"\t"+ resolvedVersion +"\t"+ protoPutMG.getVersion() 
                                +"\tuuid "+ protoPutMG.getUUID()));
                    }
                    additionResult = protoPutMG.addValue(dhtKey, value);
                    if (additionResult == ValueAdditionResult.MessageGroupFull) {
                        // If we couldn't add this key/value to the current ProtoPutMessageGroup, then we must
                        // create a new message group. Save the current group to the list of groups before that.
                        // First update the message estimate to remove keys/bytes already added.
                        estimate.addKeys(-protoPutMG.currentBufferKeys());
                        estimate.addBytes(-protoPutMG.currentValueBytes());
                        assert estimate.getNumKeys() != 0;
                        protoPutMG.addToMessageGroupList(messageGroups);
                        protoPutMG = createProtoPutMG(estimate);
                        listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), dhtKey, this);
                        if (!listenerInserted) {
                            throw new RuntimeException("Can't insert listener to new protoPutMG");
                        }
                        opUUIDs.add((OperationUUID)protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
                        additionResult = protoPutMG.addValue(dhtKey, value);
                        if (additionResult != ValueAdditionResult.Added) {
                            throw new RuntimeException("Can't add to new protoPutMG");
                        }
                    } else if (additionResult == ValueAdditionResult.ValueNeedsSegmentation) {
                        segment(key, messageGroups);
                        continue;
                        // FIXME - call continue? how to handle the opUUIDs.add() below?
                    }
                } else {
                    // The existing protoPutMG already had an entry for the given key.
                    // Create a new protoPutMG so that we can add this key.
                    estimate.addKeys(-protoPutMG.currentBufferKeys());
                    estimate.addBytes(-protoPutMG.currentValueBytes());
                    assert estimate.getNumKeys() != 0;
                    protoPutMG.addToMessageGroupList(messageGroups);
                    protoPutMG = createProtoPutMG(estimate, DHTClient.getValueCreator().getBytes());
                    opUUIDs.add((OperationUUID)protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
                    listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), dhtKey, this);
                    if (!listenerInserted) {
                        throw new RuntimeException("Can't insert listener to new protoPutMG");
                    }
                    if (debug) {
                        System.out.println("add2\t"+ dhtKey +"\t"+ resolvedVersion +"\t"+ protoPutMG.getVersion());
                    }
                    additionResult = protoPutMG.addValue(dhtKey, value);
                    if (additionResult != ValueAdditionResult.Added) {
                        throw new RuntimeException("Can't add to new protoPutMG");
                    }
                }
                opUUIDs.add((OperationUUID)protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
                //protoPutMG.addResultListener(dhtKey, this);
            }
        }
        /*
        if (protoPutMG.isNonEmpty()) {
            MessageGroup    messageGroup;
            
            messageGroup = new MessageGroup(MessageType.PUT, context.contextAsLong(), protoPutMG.getBufferList());
            messageGroups.add(messageGroup);
            messageGroup.displayForDebug();
        }
        */        
        setSent();
        // Return the current protoPutMG so that subsequent operations can use it. Some
        // subsequent operation or the sender will add it to the list of message groups.
        //if (segmentsCreated != oldSegmentsCreated) {
            // since this operation may stretch across messages, we recompute the timeout state.
            // FUTURE - consider removing this.
            // REMOVED FOR NOW, ONLY INCLUDE IF LATER DETERMINE THAT WE NEED IT
            //recomputeTimeoutState();
        //}
        
        return protoPutMG;
    }
    
    private void segment(K key, List<MessageGroup> messageGroups) {
        int         numSegments;
        int         valueSize;
        DHTKey      dhtKey;
        DHTKey[]    subKeys;
        ByteBuffer      buf;
        ByteBuffer[]    subBufs;
        ProtoPutMessageGroup<V>  protoPutMG;
        Compression compression;
        SegmentedPutValue   segmentedPutValue;
        boolean listenerInserted;
        int uncompressedLength;
        int storedLength;
        byte[]      checksum;
        
        Log.fine("segmenting: ", key);
        
        // Serialize the value and compress if needed
        buf = nspoImpl.getValueSerializer().serializeToBuffer(putOperation.getValue(key));
        uncompressedLength = buf.limit();
        compression = putOperation.putOptions().getCompression();
        if (compression != Compression.NONE) {
            Compressor  compressor;
            byte[]      compressedValue;
            
            compressor = CodecProvider.getCompressor(compression);
            try {
                compressedValue = compressor.compress(buf.array(), buf.position(), buf.remaining());
                buf = ByteBuffer.wrap(compressedValue);
            } catch (IOException ioe) {
                throw new RuntimeException("Compression error in segmentation", ioe);
            }
        }        
        storedLength = buf.limit();
        
        // Checksum the value
        // For segmented values we do not compute a complete checksum, but
        // instead we use the piecewise checksums. The primary reason for this is to allow for the
        // standard corrupt value detection/correction code to work for segmented values.
        checksum = new byte[putOperation.putOptions().getChecksumType().length()];

        // Now segment the value
        valueSize = buf.limit();
        numSegments = SegmentationUtil.getNumSegments(valueSize,  SegmentationUtil.maxValueSegmentSize);
        segmentsCreated += numSegments;
        subBufs = new ByteBuffer[numSegments];
        for (int i = 0; i < numSegments; i++) {
            ByteBuffer  subBuf;
            int         segmentStart;
            int         segmentSize;
            
            segmentStart = i * SegmentationUtil.maxValueSegmentSize; 
            segmentSize = Math.min(SegmentationUtil.maxValueSegmentSize, valueSize - segmentStart);
            buf.position(segmentStart);
            subBuf = buf.slice();
            subBuf.limit(segmentSize);
            subBufs[i] = subBuf;
            if (debugSegmentation) {
                System.out.printf("%d\t%d\t%s\n", segmentStart, segmentSize, subBufs[i]);
            }
        }
        
        dhtKey = keyCreator.createKey(key);
        subKeys = keyCreator.createSubKeys(dhtKey, numSegments);
        
        if (segmentedPutValues == null) {
            segmentedPutValues = new LinkedList<>();
        }
        segmentedPutValue = new SegmentedPutValue(subKeys, dhtKey, this);
        segmentedPutValues.add(segmentedPutValue);
        
        // NEED TO ALLOW FOR MULTIPLE PROTOPUTMG SINCE SEGMENT WILL LIKELY
        // BE A SINGLE MESSAGE OR LARGE PORTION
        
        // Create the message groups and add them to the list
        // For now, assume only one segment per message
        for (int i = 0; i < numSegments; i++) {
            byte[]  segmentChecksum;
            
            protoPutMG = createProtoPutMG(new PutMessageEstimate(1, subBufs[i].limit()));
            opUUIDs.add((OperationUUID)protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC            
            listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), subKeys[i], segmentedPutValue);
            if (!listenerInserted) {
                throw new RuntimeException("Panic: Unable to insert listener into dedicated segment protoPutMG");
            }
            if (debugSegmentation) {
                System.out.printf("segmentation listener: %s\t%s\t%s\n", 
                protoPutMG.getUUID(), subKeys[i], subBufs[i]);
                //System.out.printf("segmentation listener: %s\t%s\t%s\n", 
                        //protoPutMG.getUUID(), subKeys[i], StringUtil.byteBufferToHexString(subBufs[i]));
            }
            protoPutMG.addValueDedicated(subKeys[i], subBufs[i]);
            protoPutMG.addToMessageGroupList(messageGroups);
            segmentChecksum = new byte[putOperation.putOptions().getChecksumType().length()];
            protoPutMG.getMostRecentChecksum(segmentChecksum);
            ArrayUtil.xor(checksum, segmentChecksum);
        }        
        
        // Now add the index key/value
        // indicate segmentation by storing segmentationBytes in the creator field 
        protoPutMG = createProtoPutMG(new PutMessageEstimate(1, SegmentationUtil.segmentedValueBufferLength), MetaDataConstants.segmentationBytes);
        opUUIDs.add((OperationUUID)protoPutMG.getUUID()); // hold a reference to the uuid to prevent GC
        listenerInserted = activePutListeners.addListener(protoPutMG.getUUID(), dhtKey, segmentedPutValue);
        if (!listenerInserted) {
            throw new RuntimeException("Panic: Unable to add index key/value into dedicated protoPutMG");
        }
        if (debug) {
            System.out.printf("added index listener %s %s\n", protoPutMG.getUUID(), new SimpleKey(dhtKey));
        }
        ByteBuffer  segmentMetaDataBuffer;
        
        segmentMetaDataBuffer = SegmentationUtil.createSegmentMetaDataBuffer(DHTClient.getValueCreator().getBytes(), 
                storedLength, uncompressedLength, 
                putOperation.putOptions().getChecksumType(), checksum);
        protoPutMG.addValueDedicated(dhtKey, segmentMetaDataBuffer);
        protoPutMG.addToMessageGroupList(messageGroups);
    }
    
    @Override
    protected OpResult getOpResult(K key) {
        OpResult    opResult;
        
        opResult = opResults.get(keyToDHTKey.get(key));
        if (opResult == null) {
            return OpResult.INCOMPLETE;
        } else {
            return opResult;
        }
    }
    
    @Override
    public void resultUpdated(DHTKey key, OpResult opResult) {
        OpResult    previous;
        
        //System.out.println(key +"\t"+ opResult +"\t"+ resolvedVersion +"\t"+ this);
        Log.fine("resultUpdated ", key);
        previous = opResults.putIfAbsent(key, opResult);
        if (previous != null && previous != opResult) {
            if (debug) {
                Log.warning(String.format("resultUpdated %s new %s prev %s", key, opResult, previous));
            }
            switch (previous.toOperationState()) {
            case INCOMPLETE: break; // no action necessary
            case FAILED:
                opResults.put(key, previous);
                if (opResult.toOperationState() == OperationState.FAILED) {
                    Log.info("Multiple failures: ", key);
                } else if (opResult.toOperationState() == OperationState.SUCCEEDED) {
                    Log.warning("ActivePutOperationImpl received failure then success for: "+ key);
                    Log.warning(previous);
                }
                break;
            case SUCCEEDED:
                if (opResult.toOperationState() == OperationState.FAILED) {
                    Log.warning("ActivePutOperationImpl received success then failure for: "+ key);
                    Log.warning(opResult);
                }
                break;
            default: throw new RuntimeException("panic");
            }
        } else {
            if (opResult.hasFailed()) {
                setFailureCause(key, opResult.toFailureCause(getNonExistenceResponse()));
            }
        }
        if (resultsReceived.incrementAndGet() >= putOperation.size()) {
            //System.out.println(resultMessagesReceived +" > "+ putOperation.size());
            checkForCompletion();
        }
    }
	
    @Override
    public OperationState getOperationState(K key) {
        OpResult    result;
        
        result = opResults.get(keyToDHTKey.get(key));
        return result == null ? OperationState.INCOMPLETE : result.toOperationState();
    }

    @Override
    protected void throwFailedException() throws OperationException {
        throw new PutExceptionImpl((Map<Object,OperationState>)getOperationStateMap(), 
                                    (Map<Object,FailureCause>)getFailureCauses());
    }

    @Override
    public void waitForCompletion() throws PutException {
        try {
            _waitForCompletion();
        } catch (OperationException oe) {
            throw (PutException)oe;
        }
    }

    @Override
    public void resultReceived(DHTKey key, OpResult result) {
        if (debug) {
            Log.warning(String.format("resultReceived %s %s\n", key, result));
        }
        resultUpdated(key, result);
    }
    
    protected void debugTimeout() {
        /*
        for (Map.Entry<UUIDBase,ConcurrentMap<DHTKey,ActiveOperationResultListener<OpResult>>> entry : activePutListeners.entrySet()) {
            ConcurrentMap<DHTKey,ActiveOperationResultListener<OpResult>>   keyMap;
            
            keyMap = entry.getValue();
            for (DHTKey key : keyMap.keySet()) {
                OpResult    result;
                
                result = opResults.get(key);
                if (result == null || !result.isComplete()) {
                    System.out.printf("Incomplete: %s\t%s\n", key, entry.getKey());
                }
            }
        }
        */
        /**/
        for (OperationUUID opUUID : opUUIDs) {
            ConcurrentMap<DHTKey,WeakReference<ActiveKeyedOperationResultListener<OpResult>>>   keyMap;
            
            System.out.println("opUUID:\t"+ opUUID);
            //keyMap = activePutListeners.get(opUUID);
            keyMap = activePutListeners.getKeyMap(opUUID);
            for (DHTKey key : keyMap.keySet()) {
                if (keyMap.get(key).get() == this) {
                    OpResult    result;
                    
                    result = opResults.get(key);
                    if (result == null || !result.isComplete()) {
                        System.out.printf("IncompleteA: %s\t%s\n", opUUID, key);
                    }
                } else {
                    System.out.println(key +" -> "+ keyMap.get(key));
                }
            }
        }
        /**/
        for (DHTKey key : dhtKeys) {
            OpResult    result;
            
            result = opResults.get(key);
            if (result == null || !result.isComplete()) {
                System.out.printf("IncompleteB: %s\t%d\n", key, creationCalls);
            }
        }
    }
    
    @Override
    public boolean canBeGroupedWith(AsyncOperationImpl asyncOperationImpl) {
        if (asyncOperationImpl instanceof AsyncPutOperationImpl) {
            AsyncPutOperationImpl other;
            
            other = (AsyncPutOperationImpl)asyncOperationImpl;
            return getResolvedVersion() == other.getResolvedVersion()
                    && putOperation.putOptions().equals(other.putOperation.putOptions());
        } else {
            return false;
        }
    }
    
    @Override
    public String toString() {
        if (verboseToString) {
            StringBuilder   sb;
            
            sb = new StringBuilder();
            sb.append(super.toString());
            sb.append('\n');
            //for (Map.Entry<DHTKey, OpResult> keyAndResult : opResults.entrySet()) {
            //  sb.append(String.format("%s\t%s\n", keyAndResult.getKey(), keyAndResult.getValue()));
            //}
            for (K key : putOperation.getKeys()) {
                sb.append(String.format("%s\t%s\t%s\n", key, keyToDHTKey.get(key), getOperationState(key)));
            }
            return sb.toString();
        } else {
            return super.toString();
        }
    }
}

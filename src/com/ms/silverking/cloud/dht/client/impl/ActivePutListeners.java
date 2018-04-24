package com.ms.silverking.cloud.dht.client.impl;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hibernate.validator.internal.util.ConcurrentReferenceHashMap;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyOrdinalEntry;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.util.Libraries;

/**
 * Maps UUIDs from active messages back to active operations.
 * The typing of the mapping is complex enough that we hide 
 * it in this class.
 * 
 * The strategy of this implemention is to allow GC and weak references
 * to handle the removal of entries from the map. This removes the need to
 * perform any manual deletion.
 */
class ActivePutListeners {
    private static final boolean    enableMultipleOpsPerMessage = OpSender.opGroupingEnabled;
    
    private final ConcurrentMap<UUIDBase,ActiveKeyedOperationResultListener<OpResult>>    activeOpListeners;
    
    private final Map<UUIDBase,ConcurrentMap<DHTKey,WeakReference<ActiveKeyedOperationResultListener<OpResult>>>>   activePutListeners;
    
    private static final boolean    debug = false;

    ActivePutListeners() {
        if (enableMultipleOpsPerMessage) {
        	if (Libraries.useCustomGuava) {
                activePutListeners = new ConcurrentReferenceHashMap<>();      
        	} else {
                activePutListeners = Collections.synchronizedMap(new WeakHashMap<UUIDBase,ConcurrentMap<DHTKey,WeakReference<ActiveKeyedOperationResultListener<OpResult>>>>());        
        	}
            this.activeOpListeners = null;
        } else {
            this.activePutListeners = null;
            this.activeOpListeners = new ConcurrentHashMap<>();
        }        
    }
    
    OperationUUID newOpUUIDAndMap() {
        OperationUUID   opUUID;
        
        opUUID = new OperationUUID();
        if (enableMultipleOpsPerMessage) {
        	activePutListeners.put(opUUID, new ConcurrentHashMap<DHTKey,WeakReference<ActiveKeyedOperationResultListener<OpResult>>>());
        }
        return opUUID;
    }

    /**
     * 
     * @param opUUID
     * @param dhtKey
     * @param listener
     * @return
     */
    boolean addListener(UUIDBase opUUID, DHTKey dhtKey, ActiveKeyedOperationResultListener<OpResult> listener) {
        if (enableMultipleOpsPerMessage) {
        	return activePutListeners.get(opUUID).putIfAbsent(dhtKey, new WeakReference<>(listener)) == null;
        } else {
            Object  prev;
            
        	prev = activeOpListeners.putIfAbsent(opUUID, listener);
            if (prev != null && prev != listener) {
            	Log.warning(prev);
            	Log.warning(listener);
                throw new RuntimeException("Attempted to add multiple ops in a message, "
                        +"but enableMultipleOpsPerMessage is false");
            }
            return true;
        }
    }
    
    Set<AsyncPutOperationImpl> currentPutSet() {
        ImmutableSet.Builder<AsyncPutOperationImpl>   ops;

        Log.fine("currentPutSet()");
        ops = ImmutableSet.builder();
        if (enableMultipleOpsPerMessage) {
	        for (ConcurrentMap<DHTKey,WeakReference<ActiveKeyedOperationResultListener<OpResult>>> map : activePutListeners.values()) {
	            Log.fine("maps: ", map);
	            for (WeakReference<ActiveKeyedOperationResultListener<OpResult>> listenerRef : map.values()) {
	                ActiveKeyedOperationResultListener<OpResult> listener;
	                
	                listener = listenerRef.get();
	                Log.fine("listener: ", listener);
	                if (listener instanceof AsyncPutOperationImpl) {
	                    ops.add((AsyncPutOperationImpl)listener);
	                }
	            }
	        }
        } else {
            for (ActiveKeyedOperationResultListener<OpResult> listener : activeOpListeners.values()) {
                if (listener instanceof AsyncPutOperationImpl) {
                    ops.add((AsyncPutOperationImpl)listener);
                }
            }            
        }
        return ops.build();
    }
    
    public <K,V> void receivedPutResponse(MessageGroup message) {
        if (enableMultipleOpsPerMessage) {
	        long    version;
	        ConcurrentMap<DHTKey,WeakReference<ActiveKeyedOperationResultListener<OpResult>>> listenerMap;
	        
	        version = message.getBuffers()[0].getLong(0);
	        //Log.warning("receivedPutResponse version ", version);
	        
	        listenerMap = activePutListeners.get(message.getUUID());
	        if (listenerMap != null) {
	            for (MessageGroupKeyOrdinalEntry entry : message.getKeyOrdinalIterator()) {
	                WeakReference<ActiveKeyedOperationResultListener<OpResult>> listenerRef;
	                ActiveKeyedOperationResultListener<OpResult> listener;
	                
	                if (debug) {
	                    System.out.println(new SimpleKey(entry.getKey()));
	                }
	                listenerRef = listenerMap.get(entry.getKey());
	                listener = listenerRef.get();
	                if (listener != null) {
	                    listener.resultReceived(entry.getKey(), EnumValues.opResult[entry.getOrdinal()]);
	                } else {
	                    Log.info("receivedPutResponse. null listener ref for: ", message.getUUID() +"\t"+ entry.getKey());
	                }
	            }
	        } else {
	            // If we're receiving the error, then it's possible that we lost the
	            // reference that was stored in the weak map.
	            Log.warning("receivedPutResponse. No listenerMap for: ", message.getUUID());
	        }
        } else {
            ActiveKeyedOperationResultListener<OpResult>	listener;
            
            listener = activeOpListeners.get(message.getUUID());
            
            if (listener == null) {
                Log.info("receivedRetrievalResponse. No listener for uuid: ", message.getUUID());
            } else {
                for (MessageGroupKeyOrdinalEntry entry : message.getKeyOrdinalIterator()) {            
                    listener.resultReceived(entry.getKey(), EnumValues.opResult[entry.getOrdinal()]);
                }
            }
        }
    }

    public ConcurrentMap<DHTKey, WeakReference<ActiveKeyedOperationResultListener<OpResult>>> getKeyMap(OperationUUID opUUID) {
        if (enableMultipleOpsPerMessage) {
        	return activePutListeners.get(opUUID);
        } else {
        	return new ConcurrentHashMap<>();
        }
    }
}

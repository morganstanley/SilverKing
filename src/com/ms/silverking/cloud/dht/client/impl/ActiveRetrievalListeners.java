package com.ms.silverking.cloud.dht.client.impl;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.util.Libraries;

import org.hibernate.validator.internal.util.ConcurrentReferenceHashMap;


/**
 * Maps UUIDs from active messages back to active operations. This is necessary since we allow multiple
 * operations to be combined in a single message to improve performance.
 * 
 * The typing of the mapping is complex enough that we hide it in this class.
 * 
 * The strategy of this implementation is to allow GC and weak references
 * to handle the removal of entries from the map. This removes the need to
 * perform any manual deletion.
 */
class ActiveRetrievalListeners {
    
    private static final boolean    enableMultipleOpsPerMessage = OpSender.opGroupingEnabled;

    private final ConcurrentMap<UUIDBase,ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>    activeOpListeners;
    
    // UUIDBase-->
    //            DHTKey-->
    //                     List<ActiveKeyedOperationResultListener...>
    private final Map<UUIDBase,ConcurrentMap<DHTKey,List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>>> activeRetrievalListeners;

    private static final int    mapCapacity = 8; // FUTURE - use something non-static
    private static final int    keyMapConcurrencyLevel = 2;
    
    ActiveRetrievalListeners() {
        if (enableMultipleOpsPerMessage) {
        	if (Libraries.useCustomGuava) {
        		this.activeRetrievalListeners = new ConcurrentReferenceHashMap<>();
        	} else {
        		this.activeRetrievalListeners = Collections.synchronizedMap(new WeakHashMap<UUIDBase,ConcurrentMap<DHTKey,List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>>>());
        	}
            this.activeOpListeners = null;
        } else {
            this.activeRetrievalListeners = null;
            this.activeOpListeners = new ConcurrentHashMap<>();
        }
    }
    
    OperationUUID newOpUUID() {
        OperationUUID   opUUID;
        
        opUUID = new OperationUUID();
        if (enableMultipleOpsPerMessage) {
            activeRetrievalListeners.put(opUUID, new ConcurrentHashMap<DHTKey,List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>>(mapCapacity, keyMapConcurrencyLevel));
        }
        return opUUID;
    }
    
    /**
     * 
     * @param opUUID
     * @param dhtKey
     * @param listener
     * @return true if a new list was created for this key, false if a list already existed for this key
     */
    boolean addListener(UUIDBase opUUID, DHTKey dhtKey, 
            ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> listener) {
        if (enableMultipleOpsPerMessage) {
            List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>> listenerList;
            List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>> existingList;
            boolean newListCreated;
            
            listenerList = new LinkedList<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>();
            existingList = activeRetrievalListeners.get(opUUID).putIfAbsent(dhtKey, listenerList);
            if (existingList != null) {
                listenerList = existingList;
                newListCreated = false;
            } else {
                newListCreated = true;
            }
            listenerList.add(new WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>(listener));
            // caller should call protoMG.addKey(dhtKey) newListCreated is true
            return newListCreated;
        } else {
            Object  prev;
            
            prev = activeOpListeners.put(opUUID, listener);
            if (prev != null && prev != listener) {
            	Log.warning(prev);
            	Log.warning(listener);
                throw new RuntimeException("Attempted to add multiple ops in a message, "
                        +"but enableMultipleOpsPerMessage is false");
            }
            return true;
        }
    }
    
    Set<AsyncRetrievalOperationImpl> currentRetrievalSet() {
        ImmutableSet.Builder<AsyncRetrievalOperationImpl>   ops;

        Log.fine("currentRetrievalSet()");
        ops = ImmutableSet.builder();
        if (enableMultipleOpsPerMessage) {
            for (ConcurrentMap<DHTKey,List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>> map : activeRetrievalListeners.values()) {
                Log.fine("maps: ", map);
                // FUTURE - we can encounter concurrent modifications here
                // think about tolerating vs. preventing
                try {
                    for (List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>> list : map.values()) {
                        for (WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>> listenerRef : list) {
                            ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>   listener;
                            
                            listener = listenerRef.get();
                            Log.fine("listener: ", listener);
                            if (listener instanceof AsyncRetrievalOperationImpl) {
                                ops.add((AsyncRetrievalOperationImpl)listener);
                            }
                        }
                    }
                } catch (Exception e) {
                    // Currently, we tolerate concurrent modification-induced exceptions
                    // Not critical if we skip a retry. In the future, however, we should probably prevent this.
                    if (Log.levelMet(Level.INFO)) {
                        e.printStackTrace();
                        Log.info("Ignoring exception during currentRetrievalSet()");
                    }
                }
            }
        } else {
            for (ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> listener : activeOpListeners.values()) {
                if (listener instanceof AsyncRetrievalOperationImpl) {
                    ops.add((AsyncRetrievalOperationImpl)listener);
                }
            }            
        }
        return ops.build();
    }
    
    void receivedRetrievalResponse(MessageGroup message) {
        if (enableMultipleOpsPerMessage) {
            //long    version;
            ConcurrentMap<DHTKey,List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>>> listenerMap;
            
            //version = message.getBuffers()[0].getLong(0);
            //System.out.println("receivedRetrievalResponse version "+ version);
            
            listenerMap = activeRetrievalListeners.get(message.getUUID());
            if (listenerMap != null) {            
                //System.out.println("listenerMap "+ listenerMap);
                for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator()) {
                    List<WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>>> listenerList;
                    
                    //System.out.println("entry:\t"+ entry);
                    listenerList = listenerMap.get(entry);
                    if (listenerList != null) {
                        for (WeakReference<ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>> listenerRef : listenerList) {
                            ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>   listener;
                            
                            listener = listenerRef.get();
                            if (listener != null) {
                                listener.resultReceived(entry, entry);
                            } else {
                                Log.info("receivedRetrievalResponse. null listenerRef.get() for entry: ", entry);
                            }
                        }
                    } else {
                        Log.warning("receivedRetrievalResponse. No listener for entry: ", entry);
                    }
                }
            } else {
                // If we're receiving the error, then it's possible that we lost the
                // reference that was stored in the weak map.
                // FUTURE - was WARNING, think about level
                Log.info("receivedRetrievalResponse. No listenerMap for: ", message.getUUID());
            }
        } else {
            ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry>   listener;
            
            listener = activeOpListeners.get(message.getUUID());
            
            if (listener == null) {
                Log.info("receivedRetrievalResponse. No listener for uuid: ", message.getUUID());
            } else {
                for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator()) {            
                    listener.resultReceived(entry, entry);
                }
            }
        }
    }    
}

package com.ms.silverking.cloud.dht.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.id.UUIDBase;

import org.hibernate.validator.internal.util.ConcurrentReferenceHashMap;


/**
 * Used by both client and server to prevent duplicate operation
 * sends.
 * 
 * Currently, this is only applied to receive operations
 */
public class ActiveOperationTable<R extends Retrieval> {
    //private final ConcurrentMap<DHTKey,RetrievalGroup> activeRetrievals;
    private final ConcurrentMap<UUIDBase,Retrieval>  activeRetrievals;
    //private final ConcurrentMap<DHTKey,RetrievalGroup> activeRetrievals;
    
    // FUTURE - RetrievalGroup code is currently disabled. Think about whether or
    // not we want to add it back in.
    
    public ActiveOperationTable() {
        //activeRetrievals = new MapMaker().weakKeys().concurrencyLevel(2).makeMap();
        // FUTURE - think about cleanup and how it will work for clients and proxy servers
        activeRetrievals = new ConcurrentHashMap<>();
    }
    
    //public RetrievalGroup getRetrievalGroup(DHTKey key) {
    //    return activeRetrievals.get(key);
    //}
    
    public void addRetrieval(Retrieval retrieval) {
        /*
        int overlapped;
        
        overlapped = 0;
        for (DHTKey key : retrieval.getDHTKeys()) {
            RetrievalGroup  rGroup;
            boolean         overlapping;
            
            rGroup = activeRetrievals.get(key);
            if (rGroup == null) {
                RetrievalGroup  existingGroup;
                
                rGroup = new RetrievalGroup();
                existingGroup = activeRetrievals.putIfAbsent(key, rGroup);
                if (existingGroup != null) {
                    rGroup = existingGroup;
                }
            }
            //System.out.println(key +"\t => \t"+ rGroup);
            overlapping = rGroup.addRetrieval(retrieval);
            if (overlapping) {
                overlapped++;
            }
        }
        System.out.printf("overlapped %d/%d\n", overlapped, retrieval.getDHTKeys().size());
        System.out.printf("activeRetrievals.size() %d\n", activeRetrievals.size());
        */
        activeRetrievals.put(retrieval.getUUID(), retrieval);
    }
    
    public void receivedRetrievalResponse(MessageGroup message) {
        throw new RuntimeException("legacy code invoked");
        /*
        Retrieval   retrieval;
        
        retrieval = activeRetrievals.get(message.getUUID());
        if (retrieval != null) {
            for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator(MessageGroupKVEntry.ValueMode.Local)) {
                retrieval.addResponse(entry);
            }
            if (retrieval.getState() != OperationState.INCOMPLETE) {
                activeRetrievals.remove(message.getUUID());
            }
        } else {
            Log.warning("No active retrieval found for: ", message.getUUID());
        }
        */
        /*
        Log.fine("in receivedRetrievalResponse()");
        for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator()) {
            RetrievalGroup  rGroup;
            
            rGroup = activeRetrievals.get(entry);
            if (rGroup == null) {
                Log.warning("No RetrievalGroup for ", entry);
            } else {
                rGroup.receivedResponse(entry);
                if (rGroup.isComplete()) {
                    activeRetrievals.remove(entry);
                }
            }
        }
        Log.fine("out receivedRetrievalResponse()");
        */
    }    
}

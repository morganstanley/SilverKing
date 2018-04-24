package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.DHTKeyComparator;
import com.ms.silverking.id.UUIDBase;

class PendingWaitFor implements Comparable<PendingWaitFor> {
    private final DHTKey            key;
    private final RetrievalOptions  options;
    private final UUIDBase          opUUID;
    
    // No timeout here as this is cleaned up when the corresponding active retrieval is no longer found
    
    PendingWaitFor(DHTKey key, RetrievalOptions options, UUIDBase opUUID) {
        this.key = key;
        this.options = options;
        this.opUUID = opUUID;
    }
    
    public DHTKey getKey() {
        return key;
    }

    public RetrievalOptions getOptions() {
        return options;
    }
    
    public UUIDBase getOpUUID() {
        return opUUID;
    }
    
    @Override
    public String toString() {
        return key +":"+ options +":"+ opUUID;
    }
    
    @Override
    public int hashCode() {
        return key.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        PendingWaitFor  oWaitFor;
        
        oWaitFor = (PendingWaitFor)other;
        return key.equals(oWaitFor.key) && options.equals(oWaitFor.options) && opUUID.equals(oWaitFor.opUUID);
    }

    @Override
    public int compareTo(PendingWaitFor o) {
        int val;
        
        val = DHTKeyComparator.dhtKeyComparator.compare(key, o.key);
        if (val == 0) {
            return opUUID.compareTo(o.opUUID);
        } else {
            return val;
        }
    }
}

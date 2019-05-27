package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.net.IPAndPort;

/**
 * For a given storage message entry, the state of a StorageOperation.
 * Tracks the state at each replica.
 */
class StorageEntryPerReplicaState<K> {
    private final Map<IPAndPort,K>    replicaState;
    
    StorageEntryPerReplicaState() {
        replicaState = new HashMap<>();
    }
    
    K getReplicaState(IPAndPort replica) {
        return replicaState.get(replica);
    }
    
    void setReplicaState(IPAndPort replica, K state) {
        replicaState.put(replica, state);
    }
}

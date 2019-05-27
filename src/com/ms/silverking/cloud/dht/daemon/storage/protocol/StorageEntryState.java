package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.common.OpResult;

/**
 * For a given storage message entry, the state of a StorageOperation.
 * Tracks the state at each replica.
 */
abstract class StorageEntryState {
    private static final int    relTimeoutMillis = 100; // FIXME - make configurable
    static final int            minRelTimeoutMillis = relTimeoutMillis;
    
    StorageEntryState() {
    }
    
    abstract OpResult getCurOpResult();
}

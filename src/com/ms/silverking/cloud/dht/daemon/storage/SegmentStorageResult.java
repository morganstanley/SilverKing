package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.common.OpResult;

/**
 * Result of segment storage calls internal to storage module.   
 */
enum SegmentStorageResult {
    stored, segmentFull, mutation, invalidVersion, duplicateStore, previousStoreIncomplete;
    
    boolean callerShouldRetry() {
        return this == segmentFull;
    }
    
    OpResult toOpResult() {
        switch (this) {
        case stored: return OpResult.SUCCEEDED;
        case mutation: return OpResult.MUTATION;
        case invalidVersion: return OpResult.INVALID_VERSION;
        case duplicateStore: return OpResult.SUCCEEDED;
        case previousStoreIncomplete:
        case segmentFull: 
        default: throw new RuntimeException("panic");
        }
    }    
}

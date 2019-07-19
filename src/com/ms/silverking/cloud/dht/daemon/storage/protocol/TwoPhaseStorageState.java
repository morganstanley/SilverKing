package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.common.OpResult;

enum TwoPhaseStorageState {
    INITIAL, PREPARED, COMMITTED, STORED, FAILED;
    // STORED - is only used for tracking the protocol; not stored with value
    
    public static final TwoPhaseStorageState[]   values = values();
    
    OpResult toOpResult() {
        switch (this) {
        case INITIAL: return OpResult.INCOMPLETE;
        case PREPARED: return OpResult.INCOMPLETE;
        case COMMITTED: return OpResult.INCOMPLETE;
        case STORED: return OpResult.SUCCEEDED;
        case FAILED: return OpResult.ERROR;
        default: throw new RuntimeException("Panic");
        }
    }
    
    public boolean updateAllowed(TwoPhaseStorageState newState) {
        switch (this) {
        case INITIAL: return newState != INITIAL;
        case PREPARED: return newState.ordinal() >= this.ordinal();
        case COMMITTED: return newState == STORED;
        case STORED: return newState == STORED;
        case FAILED: return newState == FAILED;
        default: throw new RuntimeException("Panic");
        }
    }
    
    public static TwoPhaseStorageState nextState(TwoPhaseStorageState prevState) {
        switch (prevState) {
        case INITIAL: return TwoPhaseStorageState.PREPARED;
        case PREPARED: return TwoPhaseStorageState.COMMITTED;
        case COMMITTED: return TwoPhaseStorageState.STORED;
        case STORED: return TwoPhaseStorageState.STORED;
        case FAILED: return TwoPhaseStorageState.FAILED;
        default: throw new RuntimeException("panic");
        }
    }
    
    public boolean validForRead() {
        return this == STORED || this == COMMITTED; 
    }

    public boolean isComplete() {
        return this == STORED;
    }
}

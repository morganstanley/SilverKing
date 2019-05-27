package com.ms.silverking.cloud.dht.daemon.storage.protocol;

enum RetrievalState {
    INITIAL, SECONDARY_NO_SUCH_VALUE, SECONDARY_FAILED, NO_SUCH_VALUE, CORRUPT, SUCCEEDED;

    public boolean validTransition(RetrievalState state) {
        switch (this) {
        case INITIAL: return state != INITIAL;
        case SECONDARY_NO_SUCH_VALUE: return state != INITIAL;
        case SECONDARY_FAILED: return state != INITIAL;
        case NO_SUCH_VALUE: return state == this;
        case CORRUPT: return state == this;
        case SUCCEEDED: return state == this;
        default: throw new RuntimeException("panic");
        }
    }
    
    public boolean isComplete() {
        switch (this) {
        case INITIAL: // fall through
        case SECONDARY_NO_SUCH_VALUE: // fall through
        case SECONDARY_FAILED: return false;
        case NO_SUCH_VALUE: // fall through
        case CORRUPT: // fall through
        case SUCCEEDED: return true;
        default: throw new RuntimeException("panic");
        }
    }
}

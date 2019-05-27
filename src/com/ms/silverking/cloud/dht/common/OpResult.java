package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.OperationState;


/**
 * Internal representation of the result of an operation passed back by
 * methods and stored for ongoing operations.
 * 
 * The client API uses OperationState and FailureCause to obtain similar information.
 */
public enum OpResult {
    // incomplete
    INCOMPLETE,
    // success
    SUCCEEDED,
    // success and no such value
    SUCCEEDED_AND_NO_SUCH_VALUE,
    // failure
    ERROR, TIMEOUT, MUTATION, NO_SUCH_VALUE, SIMULTANEOUS_PUT, MULTIPLE, INVALID_VERSION, NO_SUCH_NAMESPACE, CORRUPT;
        
    public boolean isComplete() {
        return this != INCOMPLETE;
    }
    
    public boolean hasFailed(NonExistenceResponse nonExistenceResponse) {
    	if (this == SUCCEEDED || this == INCOMPLETE) {
    		return false;
    	} else {
    		if (this != NO_SUCH_VALUE) {
    			return true;
    		} else {
    			return nonExistenceResponse == NonExistenceResponse.EXCEPTION; 
    		}
    	}
    }
    
    public boolean hasFailed() {
    	if (this == SUCCEEDED || this == INCOMPLETE) {
    		return false;
    	} else {
    		if (this != NO_SUCH_VALUE) {
    			return true;
    		} else {
    			// For a context where NO_SUCH_VALUE may exist, the NonExistenceResponse version must be used
    			//throw new RuntimeException("Unexpected NO_SUCH_VALUE in hasFailed() ");
    			// We allow this as setting the OpResult may occur after results are added, due to async access
    			return false;
    		}
    	}
    }
    
    public OperationState toOperationState(NonExistenceResponse nonExistenceResponse) {
        switch (this) {
        case INCOMPLETE: return OperationState.INCOMPLETE;
        case SUCCEEDED: return OperationState.SUCCEEDED;
        case NO_SUCH_VALUE: return nonExistenceResponse == NonExistenceResponse.EXCEPTION ? OperationState.FAILED : OperationState.SUCCEEDED;
        default: return OperationState.FAILED;
        }
    }

    public OperationState toOperationState() {
        switch (this) {
        case INCOMPLETE: return OperationState.INCOMPLETE;
        case SUCCEEDED: return OperationState.SUCCEEDED;
		// For a context where NO_SUCH_VALUE may exist, the NonExistenceResponse version must be used
        case NO_SUCH_VALUE: throw new RuntimeException("Unexpected NO_SUCH_VALUE in toOperationState()"); 
        default: return OperationState.FAILED;
        }
    }
    
    public FailureCause toFailureCause(NonExistenceResponse nonExistenceResponse) {
        switch (this) {
        case NO_SUCH_VALUE: 
        	if (nonExistenceResponse == NonExistenceResponse.NULL_VALUE) {
        		throw new RuntimeException("toFailureCause() can't be called for "+ this +" with NonExistenceResponse.NULL_VALUE");
        	} else {
        		return FailureCause.NO_SUCH_VALUE;
        	}
        case INCOMPLETE: // fall through
        case SUCCEEDED: throw new RuntimeException("toFailureCause() can't be called for "+ this);
        case CORRUPT: return FailureCause.CORRUPT;
        case ERROR: return FailureCause.ERROR;
        case TIMEOUT: return FailureCause.TIMEOUT;
        case MUTATION: return FailureCause.MUTATION;
        case SIMULTANEOUS_PUT: return FailureCause.SIMULTANEOUS_PUT;
        case MULTIPLE: return FailureCause.MULTIPLE;
        case INVALID_VERSION: return FailureCause.INVALID_VERSION;
        case NO_SUCH_NAMESPACE: return FailureCause.NO_SUCH_NAMESPACE;
        default: throw new RuntimeException("panic");
        }
    }
    
    public FailureCause toFailureCause() {
    	return toFailureCause(null);
    }
    
    public static OpResult fromFailureCause(FailureCause failureCause) {
        switch (failureCause) {
        case CORRUPT: return CORRUPT;
        case ERROR: return ERROR;
        case TIMEOUT: return TIMEOUT;
        case MUTATION: return MUTATION;
        case SIMULTANEOUS_PUT: return SIMULTANEOUS_PUT;
        case MULTIPLE: return MULTIPLE;
        case INVALID_VERSION: return INVALID_VERSION;
        default: throw new RuntimeException("panic");
        }
    }

    public boolean supercedes(OpResult result) {
        return !isComplete() && result.isComplete();
    }

    public static boolean isIncompleteOrNull(OpResult opResult) {
        return opResult == null || opResult == INCOMPLETE;
    }
}

package com.ms.silverking.cloud.dht.client;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

/**
 * Thrown when a keyed client-initiated operation fails. OperationState is provided
 * on a key-by-key basis, but may be incomplete. At least one key will have error 
 * information available.
 */
@NonVirtual
public abstract class KeyedOperationException extends OperationException {
	private final Map<Object, OperationState>	operationState;
	private final Map<Object, FailureCause>		failureCause;
	private final Set<Object>  failedKeys;

	protected KeyedOperationException(Map<Object, OperationState> operationState, Map<Object, FailureCause> failureCause) {
		this.operationState = ImmutableMap.copyOf(operationState);
		this.failureCause = ImmutableMap.copyOf(failureCause);
		this.failedKeys = failureCause.keySet();
	}

	protected KeyedOperationException(String message, Map<Object, OperationState> operationState, Map<Object, FailureCause> failureCause) {
		super(message);
		this.operationState = ImmutableMap.copyOf(operationState);
		this.failureCause = ImmutableMap.copyOf(failureCause);
        this.failedKeys = failureCause.keySet();
	}

	protected KeyedOperationException(Throwable cause, Map<Object, OperationState> operationState, Map<Object, FailureCause> failureCause) {
		super(cause);
		this.operationState = ImmutableMap.copyOf(operationState);
		this.failureCause = ImmutableMap.copyOf(failureCause);
        this.failedKeys = failureCause.keySet();
	}

	protected KeyedOperationException(String message, Throwable cause, Map<Object, OperationState> operationState, Map<Object, FailureCause> failureCause) {
		super(message, cause);
		this.operationState = ImmutableMap.copyOf(operationState);
		this.failureCause = ImmutableMap.copyOf(failureCause);
        this.failedKeys = failureCause.keySet();
	}
		
	public Map<Object, OperationState> getOperationState() {
		return operationState;
	}
	
	public OperationState getOperationState(Object key) {
		return operationState.get(key);
	}
	
	public Map<Object, FailureCause> getFailureCause() {
		return failureCause;
	}
	
	public FailureCause getFailureCause(Object key) {
		return failureCause.get(key);
	}
	
    public Set<Object> getFailedKeys() {
        return failedKeys;
    }
    
    public String getDetailedFailureMessage() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        for (Object failedKey : getFailedKeys()) {
            sb.append(failedKey +"\t"+ getFailureCause(failedKey) +"\n");
        }
        return sb.toString();
    }
}

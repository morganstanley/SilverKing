package com.ms.silverking.cloud.dht.client;

import java.util.Map;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;


/**
 * Generated when a retrieval fails. In addition to the partial results available 
 * from KeyedOperationException, partial retrieval results may be available.
 *
 */
@NonVirtual
public abstract class RetrievalException extends KeyedOperationException {
	private final Map<Object, StoredValue>	partialResults;
	
	protected RetrievalException(Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause, 
			Map<Object, StoredValue> partialResults) {
		super(operationState, failureCause);
		this.partialResults = partialResults;
	}

	protected RetrievalException(String message,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause,
			Map<Object, StoredValue> partialResults) {
		super(message, operationState, failureCause);
		this.partialResults = partialResults;
	}

	protected RetrievalException(String message, Throwable cause,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause,
			Map<Object, StoredValue> partialResults) {
		super(message, cause, operationState, failureCause);
		this.partialResults = partialResults;
	}

	protected RetrievalException(Throwable cause,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause,
			Map<Object, StoredValue> partialResults) {
		super(cause, operationState, failureCause);
		this.partialResults = partialResults;
	}
	
	public StoredValue getStoredValue(Object key) {
		return partialResults.get(key);
	}
}

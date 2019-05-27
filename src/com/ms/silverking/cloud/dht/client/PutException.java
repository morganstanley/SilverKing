package com.ms.silverking.cloud.dht.client;

import java.util.Map;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

/**
 * Thrown when a put fails. See KeyedOperationException for details on partial
 * results.
 * 
 * @see KeyedOperationException
 *
 */
@NonVirtual
public abstract class PutException extends KeyedOperationException {
	protected PutException(Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(operationState, failureCause);
	}

	protected PutException(String message,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(message, operationState, failureCause);
	}

	protected PutException(String message, Throwable cause,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(message, cause, operationState, failureCause);
	}

	protected PutException(Throwable cause,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(cause, operationState, failureCause);
	}
}

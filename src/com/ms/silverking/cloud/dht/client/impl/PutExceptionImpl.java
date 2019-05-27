package com.ms.silverking.cloud.dht.client.impl;

import java.util.Map;

import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.KeyedOperationException;
import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.client.PutException;

/**
 * Thrown when a put fails. See KeyedOperationException for details on partial
 * results.
 * 
 * @see KeyedOperationException
 *
 */
class PutExceptionImpl extends PutException {
	PutExceptionImpl(Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(operationState, failureCause);
	}

	PutExceptionImpl(String message,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(message, operationState, failureCause);
	}

	PutExceptionImpl(String message, Throwable cause,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(message, cause, operationState, failureCause);
	}

	PutExceptionImpl(Throwable cause,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(cause, operationState, failureCause);
	}
}

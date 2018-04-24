package com.ms.silverking.cloud.dht.client;

import java.util.Map;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

@NonVirtual
public class InvalidationException extends PutException {
	public InvalidationException(Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(operationState, failureCause);
	}

	public InvalidationException(String message,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(message, operationState, failureCause);
	}

	public InvalidationException(String message, Throwable cause,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(message, cause, operationState, failureCause);
	}

	public InvalidationException(Throwable cause,
			Map<Object, OperationState> operationState,
			Map<Object, FailureCause> failureCause) {
		super(cause, operationState, failureCause);
	}
}

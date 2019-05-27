package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

/**
 * Thrown when an client-initiated DHT operation encounters an exception.
 *
 */
@NonVirtual
public abstract class OperationException extends ClientException {
	protected OperationException() {
		super();
	}

	protected OperationException(String message) {
		super(message);
	}

	protected OperationException(Throwable cause) {
		super(cause);
	}

	protected OperationException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public abstract String getDetailedFailureMessage();
}

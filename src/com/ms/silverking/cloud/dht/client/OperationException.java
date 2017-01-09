package com.ms.silverking.cloud.dht.client;

/**
 * Thrown when an client-initiated DHT operation encounters an exception.
 *
 */
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

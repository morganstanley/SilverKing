package com.ms.silverking.cloud.dht.client;

import java.util.List;

/**
 * Thrown when an exception is detected while waiting for a WaitFor operation to complete.
 */
public class WaitForCompletionException extends ClientException {
	private final List<AsyncOperation>	failedOperations;

	public WaitForCompletionException(List<AsyncOperation> failedOperations) {
		this.failedOperations = failedOperations;
	}

	public WaitForCompletionException(List<AsyncOperation> failedOperations, String cause) {
		super(cause);
		this.failedOperations = failedOperations;
	}

	public WaitForCompletionException(List<AsyncOperation> failedOperations, Throwable cause) {
		super(cause);
		this.failedOperations = failedOperations;
	}

	public WaitForCompletionException(List<AsyncOperation> failedOperations, String message, Throwable cause) {
		super(message, cause);
		this.failedOperations = failedOperations;
	}

	public List<AsyncOperation> getFailedOperations() {
		return failedOperations;
	}
}

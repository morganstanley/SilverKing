package com.ms.silverking.cloud.dht.meta;

public class IneligibleServerException extends RuntimeException {
	public IneligibleServerException() {
	}

	public IneligibleServerException(String message) {
		super(message);
	}

	public IneligibleServerException(Throwable cause) {
		super(cause);
	}

	public IneligibleServerException(String message, Throwable cause) {
		super(message, cause);
	}

	public IneligibleServerException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

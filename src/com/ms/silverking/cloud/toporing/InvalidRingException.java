package com.ms.silverking.cloud.toporing;

public class InvalidRingException extends Exception {

	public InvalidRingException() {
	}

	public InvalidRingException(String message) {
		super(message);
	}

	public InvalidRingException(Throwable cause) {
		super(cause);
	}

	public InvalidRingException(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidRingException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

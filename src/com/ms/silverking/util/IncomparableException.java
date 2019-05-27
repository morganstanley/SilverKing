package com.ms.silverking.util;

/**
 * Thrown to indicate that an attempted comparison is invalid
 */
public class IncomparableException extends RuntimeException {
	private static final long serialVersionUID = 9003263057703665275L;
	
	public IncomparableException() {
		super();
	}

	public IncomparableException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public IncomparableException(String message, Throwable cause) {
		super(message, cause);
	}

	public IncomparableException(String message) {
		super(message);
	}

	public IncomparableException(Throwable cause) {
		super(cause);
	}
}

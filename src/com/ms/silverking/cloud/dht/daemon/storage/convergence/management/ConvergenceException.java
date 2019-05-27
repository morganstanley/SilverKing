package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

public class ConvergenceException extends Exception {
	private final boolean	abandoned;
	
	private static final long serialVersionUID = -298503032285356829L;
	
	public ConvergenceException(boolean abandoned) {
		this.abandoned = abandoned;
	}
	
	public boolean getAbandoned() {
		return abandoned;
	}

	public ConvergenceException(String message) {
		super(message);
		abandoned = false;
	}

	public ConvergenceException(Throwable cause) {
		super(cause);
		abandoned = false;
	}

	public ConvergenceException(String message, Throwable cause) {
		super(message, cause);
		abandoned = false;
	}

	public ConvergenceException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		abandoned = false;
	}
}

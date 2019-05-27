package com.ms.silverking.cloud.dht.meta;

public class Patient {
	private final String	name;
	private long			lastRestartAttempt;
	private int				numRestartAttempts;
	
	private static final long	restartTimeoutMillis = 1 * 60 * 1000;
	
	public Patient(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}
	
	public long getLastRestartAttempt() {
		return lastRestartAttempt;
	}
	
	public boolean restartTimedOut(long absTimeMillis) {
		if (!restartPending()) {
			throw new RuntimeException("No restart pending");
		} else {
			return absTimeMillis - lastRestartAttempt > restartTimeoutMillis;
		}
	}

	public void handleTimeout() {
		if (!restartPending()) {
			throw new RuntimeException("No restart pending");
		} else {
			lastRestartAttempt = 0;
		}
	}

	public boolean restartPending() {
		return lastRestartAttempt > 0;
	}

	public void markRestarted(long absTimeMillis) {
		if (restartPending()) {
			throw new RuntimeException("Restart already pending");
		} else {
			++numRestartAttempts;
			lastRestartAttempt = absTimeMillis;
		}
	}
	
	public int getNumRestartAttempts() {
		return numRestartAttempts;
	}
	
	@Override
	public String toString() {
		return name +":"+ lastRestartAttempt +":"+ numRestartAttempts;
	}
}

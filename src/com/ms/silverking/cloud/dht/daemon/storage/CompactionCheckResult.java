package com.ms.silverking.cloud.dht.daemon.storage;

public class CompactionCheckResult {
	private final int	validEntries;
	private final int	invalidEntries;
	
	public CompactionCheckResult(int validEntries, int invalidEntries) {
		this.validEntries = validEntries;
		this.invalidEntries = invalidEntries;
	}
	
	public int getValidEntries() {
		return validEntries;
	}

	public int getInvalidEntries() {
		return invalidEntries;
	}
	
	public int getTotalEntries() {
		return validEntries + invalidEntries;
	}
	
	public double getInvalidFraction() {
		return (double)invalidEntries / (double)getTotalEntries();
	}
	
	@Override
	public String toString() {
		return validEntries +":"+ invalidEntries;
	}
}

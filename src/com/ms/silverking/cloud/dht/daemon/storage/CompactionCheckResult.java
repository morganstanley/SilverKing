package com.ms.silverking.cloud.dht.daemon.storage;

public class CompactionCheckResult {
  private final int validEntries;
  private final int invalidEntries;
  private final int retainedBytes;

  public CompactionCheckResult(int validEntries, int invalidEntries, int retainedBytes) {
    this.validEntries = validEntries;
    this.invalidEntries = invalidEntries;
    this.retainedBytes = retainedBytes;
  }

  public int getValidEntries() {
    return validEntries;
  }

  public int getInvalidEntries() {
    return invalidEntries;
  }

  public boolean hasInvalidEntries() {
    return invalidEntries > 0;
  }

  public int getTotalEntries() {
    return validEntries + invalidEntries;
  }

  public double getInvalidFraction() {
    return (double) invalidEntries / (double) getTotalEntries();
  }

  public int getRetainedBytes() {
    return retainedBytes;
  }

  @Override
  public String toString() {
    return validEntries + ":" + invalidEntries +":"+ retainedBytes;
  }
}

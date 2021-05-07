package com.ms.silverking.cloud.dht.daemon.storage;

/**
 * Tracks a storage entry that has been modified or deleted by the compactor
 */
public class CompactorModifiedEntry {
  private final long  version;
  private final int   rawSegmentNumber;
  private final long  creationTime;
  private final int   newSegmentNumber;

  public static final int REMOVED = -1;

  private CompactorModifiedEntry(long version, int rawSegmentNumber, long creationTime, int newSegmentNumber) {
    this.version = version;
    this.rawSegmentNumber = rawSegmentNumber;
    this.creationTime = creationTime;
    this.newSegmentNumber = newSegmentNumber;
  }

  public static CompactorModifiedEntry newRemovedEntry(long version, int sourceSegmentNumber, long creationTime) {
    return new CompactorModifiedEntry(version, sourceSegmentNumber, creationTime, REMOVED);
  }

  public static CompactorModifiedEntry newModifiedEntry(long version, int sourceSegmentNumber, long creationTime,
      int newSegmentNumber) {
    return new CompactorModifiedEntry(version, sourceSegmentNumber, creationTime, newSegmentNumber);
  }

  public long getVersion() {
    return version;
  }

  public int getRawSegmentNumber() {
    return rawSegmentNumber;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public int getNewSegmentNumber() {
    return newSegmentNumber;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(version) ^ Integer.hashCode(rawSegmentNumber)
        ^ Long.hashCode(creationTime) ^ Integer.hashCode(newSegmentNumber);
  }

  @Override
  public boolean equals(Object other) {
    CompactorModifiedEntry  o;

    o = (CompactorModifiedEntry)other;
    return version == o.version && rawSegmentNumber == o.rawSegmentNumber
        && creationTime == o.creationTime && newSegmentNumber == o.newSegmentNumber;
  }

  @Override
  public String toString() {
    return version +":"+ rawSegmentNumber +":"+ creationTime +":"+ newSegmentNumber;
  }
}

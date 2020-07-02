package com.ms.silverking.cloud.dht.daemon.storage.management;

public class PurgeResult {
  public final long versionsCount;
  public final long latestCreationTimeNanos;
  public final long latestVersion;
  public final int segmentsReaped;
  public final int segmentsTrashed;
  public final int segmentsDeleted;

  public PurgeResult(long versionsCount, long latestCreationTimeNanos, long latestVersion, int segmentsReaped,
      int segmentsTrashed, int segmentsDeleted) {
    this.versionsCount = versionsCount;
    this.latestCreationTimeNanos = latestCreationTimeNanos;
    this.latestVersion = latestVersion;
    this.segmentsReaped = segmentsReaped;
    this.segmentsTrashed = segmentsTrashed;
    this.segmentsDeleted = segmentsDeleted;
  }

  public static final int noVersion = -1;

  public static PurgeResult empty() {
    return new PurgeResult(0, noVersion, noVersion, 0, 0, 0);
  }
}

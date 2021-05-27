package com.ms.silverking.cloud.dht.daemon.storage.retention;

public final class LruRetentionInfo {
  private final long version;
  private final int compressedSizeBytes;

  public LruRetentionInfo(long version, int compressedSizeBytes) {
    this.version = version;
    this.compressedSizeBytes = compressedSizeBytes;
  }

  public long getVersion() {
    return version;
  }

  public int getCompressedSizeBytes() {
    return compressedSizeBytes;
  }
}

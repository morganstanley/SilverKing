package com.ms.silverking.cloud.skfs.dir.serverside;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;

class MergedStorageParameters implements SSStorageParameters {
  private final long version;
  private final long creationTime;
  private final byte[] valueCreator;
  private final short lockSeconds;

  MergedStorageParameters(long version, long creationTime, byte[] valueCreator, short lockSeconds) {
    this.version = version;
    this.creationTime = creationTime;
    this.valueCreator = valueCreator.clone();
    this.lockSeconds = lockSeconds;
  }

  public MergedStorageParameters(StorageValueAndParameters svp) {
    this(svp.getVersion(), svp.getCreationTime(), svp.getValueCreator(), svp.getLockSeconds());
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public int getUncompressedSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getCompressedSize() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Compression getCompression() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte getStorageState() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getChecksum() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getValueCreator() {
    return valueCreator;
  }

  @Override
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public short getLockSeconds() {
    return lockSeconds;
  }

  @Override
  public ChecksumType getChecksumType() {
    throw new UnsupportedOperationException();
  }

  public MergedStorageParameters merge(StorageParameters sp) {
    return new MergedStorageParameters(Math.max(version, sp.getVersion()), Math.max(creationTime, sp.getCreationTime()),
        sp.getValueCreator(), sp.getLockSeconds());
  }
}

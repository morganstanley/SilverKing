package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.serverside.SSStorageParametersAndRequirements;

/**
 * Wraps StorageParameters to hide internal optimizations from users of SSStorageParameters
 */
public class SSStorageParametersImpl implements SSStorageParametersAndRequirements {
  private final StorageParametersAndRequirements sp;
  private final int compressedSize;
  private final Compression compression;

  public SSStorageParametersImpl(StorageParametersAndRequirements sp, int bufferRemaining) {
    this.sp = sp;
    // Hide internal optimizations from end-users
    if (sp.compressedSizeSet()) {
      this.compressedSize = sp.getCompressedSize();
    } else {
      this.compressedSize = bufferRemaining;
    }
    if (compressedSize == sp.getUncompressedSize()) {
      compression = Compression.NONE;
    } else {
      compression = sp.getCompression();
    }
  }

  @Override
  public long getVersion() {
    return sp.getVersion();
  }

  @Override
  public long getRequiredPreviousVersion() {
    return sp.getRequiredPreviousVersion();
  }

  @Override
  public short getLockSeconds() {
    return sp.getLockSeconds();
  }

  @Override
  public int getUncompressedSize() {
    return sp.getUncompressedSize();
  }

  @Override
  public int getCompressedSize() {
    return compressedSize;
  }

  @Override
  public Compression getCompression() {
    return compression;
  }

  @Override
  public byte getStorageState() {
    return sp.getStorageState();
  }

  @Override
  public byte[] getChecksum() {
    return sp.getChecksum();
  }

  @Override
  public byte[] getValueCreator() {
    return sp.getValueCreator();
  }

  @Override
  public long getCreationTime() {
    return sp.getCreationTime();
  }

  @Override
  public ChecksumType getChecksumType() {
    return sp.getChecksumType();
  }
}

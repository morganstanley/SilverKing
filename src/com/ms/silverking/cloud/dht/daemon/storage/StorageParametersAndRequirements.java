package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.serverside.SSStorageParametersAndRequirements;

public class StorageParametersAndRequirements extends StorageParameters {
  private final long requiredPreviousVersion;

  public StorageParametersAndRequirements(long version, int uncompressedSize, int compressedSize, short ccss,
      byte[] checksum, byte[] valueCreator, long creationTime, long requiredPreviousVersion, short lockSeconds) {
    super(version, uncompressedSize, compressedSize, lockSeconds, ccss, checksum, valueCreator, creationTime);
    this.requiredPreviousVersion = requiredPreviousVersion;
  }

  public static StorageParametersAndRequirements fromSSStorageParametersAndRequirements(
      SSStorageParametersAndRequirements sp) {
    if (sp instanceof StorageParameters) {
      return (StorageParametersAndRequirements) sp;
    } else {
      return new StorageParametersAndRequirements(sp.getVersion(), sp.getUncompressedSize(), sp.getCompressedSize(),
          CCSSUtil.createCCSS(sp.getCompression(), sp.getChecksumType(), sp.getStorageState()), sp.getChecksum(),
          sp.getValueCreator(), sp.getCreationTime(), sp.getRequiredPreviousVersion(), sp.getLockSeconds());
    }
  }

  public static StorageParametersAndRequirements fromSSStorageParametersAndRequirements(
      SSStorageParametersAndRequirements sp, int uncompressedSize, int compressedSize, Compression compression) {
    if (sp instanceof StorageParameters) {
      return (StorageParametersAndRequirements) sp;
    } else {
      return new StorageParametersAndRequirements(sp.getVersion(), uncompressedSize, compressedSize,
          CCSSUtil.createCCSS(compression, sp.getChecksumType(), sp.getStorageState()), sp.getChecksum(),
          sp.getValueCreator(), sp.getCreationTime(), sp.getRequiredPreviousVersion(), sp.getLockSeconds());
    }
  }

  public long getRequiredPreviousVersion() {
    return requiredPreviousVersion;
  }

  @Override
  public String toString() {
    return super.toString() + ":" + requiredPreviousVersion;
  }
}

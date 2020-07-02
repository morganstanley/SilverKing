package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.impl.SystemChecksum;

public class SSInvalidationParametersImpl extends SSStorageParametersImpl {

  public SSInvalidationParametersImpl(StorageParametersAndRequirements sp, int bufferRemaining) {
    super(sp, bufferRemaining);
  }

  @Override
  public Compression getCompression() {
    return Compression.NONE;
  }

  @Override
  public byte[] getChecksum() {
    return SystemChecksum.getInvalidationChecksum();
  }

  @Override
  public ChecksumType getChecksumType() {
    return ChecksumType.SYSTEM;
  }
}

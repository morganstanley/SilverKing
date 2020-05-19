package com.ms.silverking.cloud.dht.serverside;

public interface SSStorageParametersAndRequirements extends SSStorageParameters {
  public long getRequiredPreviousVersion();

  public short getLockSeconds();
}

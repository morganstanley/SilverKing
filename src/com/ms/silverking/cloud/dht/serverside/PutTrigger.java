package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;

public interface PutTrigger extends Trigger {
  public OpResult put(SSNamespaceStore nsStore, DHTKey key, ByteBuffer value,
      SSStorageParametersAndRequirements storageParams, byte[] userData, NamespaceVersionMode nsVersionMode);

  public OpResult putUpdate(SSNamespaceStore nsStore, DHTKey key, long version, byte storageState);

  /**
   * Merges and puts in one step.
   * writeLock is held by caller
   *
   * @param values
   * @return results of the individual puts
   */
  public Map<DHTKey, OpResult> mergePut(List<StorageValueAndParameters> values);

  public boolean supportsMerge();
}

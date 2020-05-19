package com.ms.silverking.cloud.dht.serverside;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.KeyToReplicaResolver;

public interface SSNamespaceStore {
  public long getNamespaceHash();

  public boolean isNamespace(String ns);

  public File getNamespaceSSDir();

  public NamespaceOptions getNamespaceOptions();

  public OpResult put(DHTKey key, ByteBuffer value, SSStorageParametersAndRequirements storageParams, byte[] userData,
      NamespaceVersionMode nsVersionMode);

  public OpResult putUpdate(DHTKey key, long version, byte storageState);

  public ByteBuffer retrieve(DHTKey key, SSRetrievalOptions options);

  public ByteBuffer[] retrieve(DHTKey[] keys, SSRetrievalOptions options);

  public ReadWriteLock getReadWriteLock();

  public KeyToReplicaResolver getKeyToReplicaResolver();

  public int getHeadSegmentNumber();
}

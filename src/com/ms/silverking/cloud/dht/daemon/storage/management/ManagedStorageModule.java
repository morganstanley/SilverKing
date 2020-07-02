package com.ms.silverking.cloud.dht.daemon.storage.management;

import java.io.File;

public interface ManagedStorageModule {
  ManagedNamespaceStore getManagedNamespaceStore(String nsName) throws ManagedNamespaceNotCreatedException;

  File getBaseDir();
}

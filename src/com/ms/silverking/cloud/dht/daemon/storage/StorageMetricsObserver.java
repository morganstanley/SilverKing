package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.List;

public interface StorageMetricsObserver {
  public void initialize(List<MetricsNamespaceStore> metricsNamespaceStores);
}

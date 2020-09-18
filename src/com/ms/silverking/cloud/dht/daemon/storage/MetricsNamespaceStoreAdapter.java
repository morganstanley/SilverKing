package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Map;

public interface MetricsNamespaceStoreAdapter {
  Map<String, Map<String, String>> getAllStats();
}

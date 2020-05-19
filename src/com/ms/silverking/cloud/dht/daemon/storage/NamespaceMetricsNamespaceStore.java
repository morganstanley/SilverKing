package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.impl.KeyCreator;
import com.ms.silverking.cloud.dht.client.serialization.internal.StringMD5KeyCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.id.UUIDBase;

/**
 * Provides information regarding the local DHT Node
 */
class NamespaceMetricsNamespaceStore extends MetricsNamespaceStore {
  private final NamespaceMetrics nsMetrics;

  private static final Map<DHTKey, String> keyToNameMap;

  static {
    KeyCreator<String> keyCreator;

    keyToNameMap = new HashMap<>();
    keyCreator = new StringMD5KeyCreator();
    for (String metricName : NamespaceMetrics.getMetricNames()) {
      keyToNameMap.put(keyCreator.createKey(metricName), metricName);
    }
  }

  NamespaceMetricsNamespaceStore(MessageGroupBase mgBase, NodeRingMaster2 ringMaster,
      ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals, NamespaceStore nsStore) {
    super(Namespace.namespaceMetricsBaseName + String.format("%x", nsStore.getNamespace()), mgBase, ringMaster,
        activeRetrievals, keyToNameMap);
    this.nsMetrics = nsStore.getNamespaceMetrics();
  }

  public static boolean isMetricKey(DHTKey key) {
    return keyToNameMap.containsKey(key);
  }

  public static String keyToName(DHTKey key) {
    return keyToNameMap.get(key);
  }

  protected byte[] createDynamicValue(DHTKey key, InternalRetrievalOptions options) {
    String name;

    name = keyToNameMap.get(key);
    if (name != null) {
      return nsMetrics.getMetric(name).toString().getBytes();
    } else {
      return null;
    }
    // FUTURE - support metadata
  }

    /*
    protected ByteBuffer _retrieve(DHTKey key, InternalRetrievalOptions options) {
        Log.warningf("NamespaceMetricsNamespaceStore._retrieve() %x %s %s", getNamespace(), KeyUtil.keyToString(key),
         keyToNameMap.get(key));
        return super.retrieve(key, options);
    }
    */
}

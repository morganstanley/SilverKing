package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.client.SimpleTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.util.BufferUtil;

public abstract class MetricsNamespaceStore extends DynamicNamespaceStore {
  private final Map<DHTKey, String> keyToNameMap;

  private static final RetrievalOptions defaultMetricsRetrievalOptions = new RetrievalOptions(
      new SimpleTimeoutController(1, 60 * 1000), null, RetrievalType.VALUE, WaitMode.GET, VersionConstraint.greatest,
      NonExistenceResponse.NULL_VALUE, false, false, ForwardingMode.DO_NOT_FORWARD, false, null, null);

  MetricsNamespaceStore(String name, MessageGroupBase mgBase, NodeRingMaster2 ringMaster,
      ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals, Map<DHTKey, String> keyToNameMap) {
    super(name, mgBase, ringMaster, activeRetrievals);
    this.keyToNameMap = keyToNameMap;
  }

  MetricsNamespaceStore(String name, MessageGroupBase mgBase, NodeRingMaster2 ringMaster,
      ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals) {
    this(name, mgBase, ringMaster, activeRetrievals, new HashMap<>());
  }

  protected DHTKey createAndStoreKey(String keyName) {
    DHTKey dhtKey;

    dhtKey = keyCreator.createKey(keyName);
    keyToNameMap.put(dhtKey, keyName);
    return dhtKey;
  }

  public Map<String, String> getAllMetricsAsString() {
    return getAllMetricsAsString(defaultMetricsRetrievalOptions);
  }

  public Map<String, String> getAllMetricsAsString(RetrievalOptions options) {
    Map<String, ByteBuffer> rawMetrics;
    Map<String, String> metrics;

    rawMetrics = getAllMetrics(options);
    metrics = new HashMap<>(rawMetrics.size());
    for (Map.Entry<String, ByteBuffer> entry : rawMetrics.entrySet()) {
      metrics.put(entry.getKey(), new String(BufferUtil.arrayCopy(entry.getValue())));
    }
    return ImmutableMap.copyOf(metrics);
  }

  public Map<String, ByteBuffer> getAllMetrics() {
    return getAllMetrics(defaultMetricsRetrievalOptions);
  }

  public Map<String, ByteBuffer> getAllMetrics(RetrievalOptions options) {
    Map<String, ByteBuffer> metrics;

    metrics = new HashMap<>(keyToNameMap.size());
    for (DHTKey key : keyToNameMap.keySet()) {
      metrics.put(keyToNameMap.get(key), this._retrieve(key, options));
    }
    return ImmutableMap.copyOf(metrics);
  }
}

package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.NamespaceUtil;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStoreConstants.NodeConstants;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.util.memory.JVMMemoryObserver;

/**
 * Provides information regarding the local DHT Node
 */
class NodeNamespaceStore extends MetricsNamespaceStore implements JVMMemoryObserver, MetricsNamespaceStoreAdapter {
  private final DHTKey nodeIDKey;
  private final DHTKey bytesFreeKey;
  private final DHTKey totalNamespacesKey;
  private final DHTKey namespacesKey;
  private volatile long bytesFree;
  private final ConcurrentMap<Long, NamespaceStore> namespaces;

  private static final String nsName = Namespace.nodeName;

  NodeNamespaceStore(MessageGroupBase mgBase, NodeRingMaster2 ringMaster,
      ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals, ConcurrentMap<Long, NamespaceStore> namespaces) {
    super(nsName, mgBase, ringMaster, activeRetrievals);
    // static
    nodeIDKey = createAndStoreKey("nodeID");
    // dynamic
    bytesFreeKey = createAndStoreKey("bytesFree");
    totalNamespacesKey = createAndStoreKey("totalNamespaces");
    namespacesKey = createAndStoreKey("namespaces");
    storeSystemKVPairs(mgBase, SystemTimeUtil.skSystemTimeSource.absTimeNanos());
    this.namespaces = namespaces;
  }

  private void storeSystemKVPairs(MessageGroupBase mgBase, long curTimeMillis) {
    storeStaticKVPair(mgBase, curTimeMillis, nodeIDKey,
        IPAddrUtil.addrAndPortToString(mgBase.getIPAndPort().toByteArray()));
  }

  private boolean isFilteredNamespace(NamespaceStore nsStore) {
    return nsStore.isDynamic() || nsStore.getNamespace() == NamespaceUtil.metaInfoNamespace.contextAsLong();
  }

  protected byte[] createDynamicValue(DHTKey key, InternalRetrievalOptions options) {
    byte[] value;

    value = null;
    if (key.equals(nodeIDKey)) {
      value = IPAddrUtil.localIPString().getBytes();
    } else if (key.equals(bytesFreeKey)) {
      value = Long.toString(bytesFree).getBytes();
    } else if (key.equals(totalNamespacesKey)) {
      long totalNamespaces;

      totalNamespaces = 0;
      for (NamespaceStore nsStore : namespaces.values()) {
        if (!isFilteredNamespace(nsStore)) {
          ++totalNamespaces;
        }
      }
      value = Long.toString(totalNamespaces).getBytes();
    } else if (key.equals(namespacesKey)) {
      StringBuilder sb;

      sb = new StringBuilder();
      for (NamespaceStore nsStore : namespaces.values()) {
        if (!isFilteredNamespace(nsStore)) {
          sb.append(String.format("%x\n", nsStore.getNamespace()));
        }
      }
      value = sb.toString().getBytes();
    } else if (NamespaceMetricsNamespaceStore.isMetricKey(key)) {
      NamespaceMetrics aggregateMetrics;

      aggregateMetrics = new NamespaceMetrics();
      for (NamespaceStore nsStore : namespaces.values()) {
        if (!isFilteredNamespace(nsStore)) {
          aggregateMetrics = NamespaceMetrics.aggregate(aggregateMetrics, nsStore.getNamespaceMetrics());
        }
      }
      value = aggregateMetrics.getMetric(NamespaceMetricsNamespaceStore.keyToName(key)).toString().getBytes();
    }
    return value;
  }

  // JVMMemoryObserver implementation

  @Override
  public void jvmMemoryLow(boolean isLow) {
  }

  @Override
  public void jvmMemoryStatus(long bytesFree) {
    this.bytesFree = bytesFree;
  }

  @Override
  public Map<String, Map<String, String>> getAllStats() {
    Map<String, Map<String, String>> results = new HashMap<>();
    for (NamespaceStore nsStore : namespaces.values()) {
      if (nsStore instanceof SystemNamespaceStore | nsStore instanceof ReplicasNamespaceStore)
        continue;
      String namespace = nsStore.getNamespaceProperties().getName();
      if (namespace == null) {
        Log.fineAsync("Namespace name is null");
        if (nsStore instanceof NodeNamespaceStore) {
          namespace = Namespace.nodeName;
          Log.fineAsync("Updated namespace name is " + namespace);
        } else {
          namespace = Long.toHexString(nsStore.getNamespaceHash());
          Log.fineAsync("Updated namespace name derived from hash is " + namespace);
        }
        Log.fineAsync("Final namespace name is " + namespace);
      }
      NamespaceMetrics stats = nsStore.getNamespaceMetrics();
      Map<String, String> thisNsResults = new HashMap<>();
      thisNsResults.put(NodeConstants.nsTotalKeysVar, String.valueOf(getTotalKeys()));
      thisNsResults.put(NodeConstants.bytesFreeVar, Long.toString(bytesFree));
      thisNsResults.put(NodeConstants.nsBytesCompressedVar, String.valueOf(stats.getBytesCompressed()));
      thisNsResults.put(NodeConstants.nsBytesUncompressedVar, String.valueOf(stats.getBytesUncompressed()));
      thisNsResults.put(NodeConstants.nsTotalInvalidationsVar, String.valueOf(stats.getTotalInvalidations()));
      thisNsResults.put(NodeConstants.nsTotalPutsVar, String.valueOf(stats.getTotalPuts()));
      thisNsResults.put(NodeConstants.nsTotalRetrievalsVar, String.valueOf(stats.getTotalRetrievals()));
      results.put(namespace, thisNsResults);
    }
    return results;
  }
}

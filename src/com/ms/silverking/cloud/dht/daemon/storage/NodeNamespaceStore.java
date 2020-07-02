package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.NamespaceUtil;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.util.memory.JVMMemoryObserver;

/**
 * Provides information regarding the local DHT Node
 */
class NodeNamespaceStore extends MetricsNamespaceStore implements JVMMemoryObserver {
  private final DHTKey nodeIDKey;
  private final DHTKey bytesFreeKey;
  private final DHTKey totalNamespacesKey;
  private final DHTKey namespacesKey;
  private volatile long bytesFree;
  private final ConcurrentMap<Long, NamespaceStore> namespaces;

  private static final String nsName = Namespace.nodeName;
  static final long context = getNamespace(nsName).contextAsLong();

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
    storeStaticKVPair(mgBase, curTimeMillis, nodeIDKey, mgBase.getIPAndPort().toString());
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
}

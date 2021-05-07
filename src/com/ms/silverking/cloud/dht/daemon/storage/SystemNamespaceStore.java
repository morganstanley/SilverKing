package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.ActiveProxyRetrieval;
import com.ms.silverking.cloud.dht.daemon.NodeInfo;
import com.ms.silverking.cloud.dht.daemon.NodeRingMaster2;
import com.ms.silverking.cloud.dht.daemon.RingHealth;
import com.ms.silverking.cloud.dht.meta.NodeInfoZK;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.meta.ExclusionSet;
import org.apache.zookeeper.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.text.StringUtil;

/**
 * Provides information regarding the dht system as a whole
 */
class SystemNamespaceStore extends MetricsNamespaceStore implements MetricsNamespaceStoreAdapter {
  private final NodeInfoZK nodeInfoZK;
  private final NodeRingMaster2 ringMaster;
  private final DHTKey totalDiskBytesKey;
  private final DHTKey usedDiskBytesKey;
  private final DHTKey freeDiskBytesKey;
  private final DHTKey diskBytesKey;
  private final DHTKey allReplicasKey;
  private final DHTKey allReplicasFreeDiskBytesKey;
  private final DHTKey allReplicasFreeSystemDiskBytesEstimateKey;
  private final DHTKey exclusionSetKey;
  private final DHTKey ringHealthKey;
  private final Set<DHTKey> knownKeys;

  private Map<IPAndPort, NodeInfo> cachedAllNodeInfo;
  private long allNodeInfoCacheTimeMS;
  private static final long nodeInfoCacheTimeExpirationMS = 4 * 60 * 1000;

  private static final String nsName = Namespace.systemName;

  SystemNamespaceStore(MessageGroupBase mgBase, NodeRingMaster2 ringMaster,
      ConcurrentMap<UUIDBase, ActiveProxyRetrieval> activeRetrievals, Iterable<NamespaceStore> nsStoreIterator,
      NodeInfoZK nodeInfoZK) {
    super(nsName, mgBase, ringMaster, activeRetrievals);
    this.nodeInfoZK = nodeInfoZK;
    this.ringMaster = ringMaster;
    // static
    // no static keys in this namespace at present
    // dynamic
    totalDiskBytesKey = keyCreator.createKey(NamespaceStoreConstants.SystemConstants.totalDiskBytesVar);
    usedDiskBytesKey = keyCreator.createKey(NamespaceStoreConstants.SystemConstants.usedDiskBytesVar);
    freeDiskBytesKey = keyCreator.createKey(NamespaceStoreConstants.SystemConstants.freeDiskBytesVar);
    diskBytesKey = keyCreator.createKey(NamespaceStoreConstants.SystemConstants.diskBytesVar);
    allReplicasKey = keyCreator.createKey(NamespaceStoreConstants.SystemConstants.allReplicasVar);
    allReplicasFreeDiskBytesKey = keyCreator.createKey(
        NamespaceStoreConstants.SystemConstants.allReplicasFreeDiskBytesVar);
    allReplicasFreeSystemDiskBytesEstimateKey = keyCreator.createKey(
        NamespaceStoreConstants.SystemConstants.allReplicasFreeSystemDiskBytesEstimateVar);
    exclusionSetKey = keyCreator.createKey(NamespaceStoreConstants.SystemConstants.exclusionSetVar);
    ringHealthKey = keyCreator.createKey(NamespaceStoreConstants.SystemConstants.ringHealthVar);

    knownKeys = new HashSet<>();
    knownKeys.add(totalDiskBytesKey);
    knownKeys.add(usedDiskBytesKey);
    knownKeys.add(freeDiskBytesKey);
    knownKeys.add(diskBytesKey);
    knownKeys.add(allReplicasKey);
    knownKeys.add(allReplicasFreeDiskBytesKey);
    knownKeys.add(allReplicasFreeSystemDiskBytesEstimateKey);
    knownKeys.add(exclusionSetKey);
    knownKeys.add(ringHealthKey);
  }

  /*
   * For all regions, determine the space available for that particular region.
   * Scale that to the entire ring. This gives an estimate of available space based
   * on this region.
   *
   * Take the min of all of these estimates.
   */

  private Map<IPAndPort, NodeInfo> getAllNodeInfo() throws KeeperException {
    synchronized (this) {
      Map<IPAndPort, NodeInfo> _cachedAllNodeInfo;
      boolean refresh;

      if (cachedAllNodeInfo == null) {
        refresh = true;
      } else {
        refresh =
            SystemTimeUtil.skSystemTimeSource.absTimeMillis() > allNodeInfoCacheTimeMS + nodeInfoCacheTimeExpirationMS;
      }
      if (refresh) {
        _cachedAllNodeInfo = nodeInfoZK.getNodeInfo(ringMaster.getAllCurrentReplicaServers());
        cachedAllNodeInfo = _cachedAllNodeInfo;
        allNodeInfoCacheTimeMS = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
      } else {
        _cachedAllNodeInfo = cachedAllNodeInfo;
      }
      return _cachedAllNodeInfo;
    }
  }

  private Triple<Long, Long, Long> getDiskSpace() throws KeeperException {
    Map<IPAndPort, NodeInfo> nodeInfo;
    long minTotal;
    long minFree;

    nodeInfo = getAllNodeInfo();
    minTotal = Long.MAX_VALUE;
    minFree = Long.MAX_VALUE;
    for (Map.Entry<IPAndPort, NodeInfo> entry : nodeInfo.entrySet()) {
      Pair<Long, Long> nodeEstimate;

      if (entry.getValue() != null) {
        nodeEstimate = getNodeEstimate(entry.getKey(), entry.getValue());
        if (nodeEstimate != null) {
          minTotal = Math.min(minTotal, nodeEstimate.getV1());
          minFree = Math.min(minFree, nodeEstimate.getV2());
        }
      }
    }
    return new Triple<>(minTotal, minFree, minTotal - minFree);
  }

  private Pair<Long, Long> getNodeEstimate(IPAndPort node, NodeInfo info) {
    long totalSystemBytesEstimate;
    long freeSystemBytesEstimate;
    double currentOwnedFraction;

    currentOwnedFraction = ringMaster.getCurrentOwnedFraction(node, OwnerQueryMode.Primary);
    if (currentOwnedFraction == 0.0) {
      Log.warningf("getNodeEstimate(%s) unable to find any owned fraction. Ignoring", node.toString());
      return null;
    } else {
      totalSystemBytesEstimate = (long) ((double) info.getFSTotalBytes() / currentOwnedFraction);
      freeSystemBytesEstimate = (long) ((double) info.getFSFreeBytes() / currentOwnedFraction);
      return new Pair<>(totalSystemBytesEstimate, freeSystemBytesEstimate);
    }
  }

  private byte[] getAllReplicas() {
    List<IPAndPort> results;

    results = ImmutableList.copyOf(ringMaster.getAllCurrentReplicaServers());
    return singleResultsToBytes(results);
  }

  private byte[] getAllReplicasFreeDiskBytes() {
    List<Pair<IPAndPort, Long>> results;
    Map<IPAndPort, NodeInfo> nodeInfo;

    results = new ArrayList<>();
    try {
      nodeInfo = getAllNodeInfo();
      for (IPAndPort node : ringMaster.getAllCurrentReplicaServers()) {
        NodeInfo info;

        info = nodeInfo.get(node);
        if (info != null) {
          results.add(new Pair<>(node, info.getFSFreeBytes()));
        }
      }
    } catch (KeeperException ke) {
      Log.logErrorWarning(ke);
    }
    return pairedResultsToBytes(results);
  }

  private byte[] getAllReplicasFreeSystemDiskBytesEstimate() {
    List<Pair<IPAndPort, Long>> results;
    Map<IPAndPort, NodeInfo> nodeInfo;

    results = new ArrayList<>();
    try {
      nodeInfo = getAllNodeInfo();
      for (IPAndPort node : ringMaster.getAllCurrentReplicaServers()) {
        NodeInfo info;

        info = nodeInfo.get(node);
        if (info != null) {
          long freeSystemBytesEstimate;

          freeSystemBytesEstimate = (long) ((double) info.getFSFreeBytes() / ringMaster.getCurrentOwnedFraction(node,
              OwnerQueryMode.Primary));
          results.add(new Pair<>(node, freeSystemBytesEstimate));
        }
      }
    } catch (KeeperException ke) {
      Log.logErrorWarning(ke);
    }
    return pairedResultsToBytes(results);
  }

  private byte[] getExclusionSet() {
    ExclusionSet exclusionSet;

    exclusionSet = ringMaster.getCurrentExclusionSet();
    if (exclusionSet != null) {
      return exclusionSet.toString().getBytes();
    } else {
      return null;
    }
  }

  private byte[] getRingHealth() {
    RingHealth ringHealth;

    ringHealth = ringMaster.getRingHealth();
    if (ringHealth != null) {
      return ringHealth.toString().getBytes();
    } else {
      return null;
    }
  }

  public byte[] singleResultsToBytes(List<IPAndPort> results) {
    StringBuffer sBuf;

    sBuf = new StringBuffer();
    for (IPAndPort result : results) {
      sBuf.append(String.format("%s\n", result.getIPAsString()));
    }
    return sBuf.toString().getBytes();
  }

  public byte[] pairedResultsToBytes(List<Pair<IPAndPort, Long>> results) {
    StringBuffer sBuf;

    sBuf = new StringBuffer();
    Collections.sort(results, SpaceComparator.instance);
    for (Pair<IPAndPort, Long> result : results) {
      sBuf.append(String.format("%s\t%d\n", result.getV1().getIPAsString(), result.getV2()));
    }
    return sBuf.toString().getBytes();
  }

  private static class SpaceComparator implements Comparator<Pair<IPAndPort, Long>> {
    public SpaceComparator() {
    }

    static final SpaceComparator instance = new SpaceComparator();

    @Override
    public int compare(Pair<IPAndPort, Long> o1, Pair<IPAndPort, Long> o2) {
      int result;

      result = Long.compare(o1.getV2(), o2.getV2());
      if (result == 0) {
        result = o1.getV1().compareTo(o2.getV1());
      }
      return result; // list in ascending order
    }

  }

  protected byte[] createDynamicValue(DHTKey key, InternalRetrievalOptions options) {
    try {
      byte[] value;

      value = null;
      if (knownKeys.contains(key)) {
        Triple<Long, Long, Long> diskSpace;

        diskSpace = getDiskSpace();
        if (key.equals(totalDiskBytesKey)) {
          return Long.toString(diskSpace.getV1()).getBytes();
        } else if (key.equals(freeDiskBytesKey)) {
          return Long.toString(diskSpace.getV2()).getBytes();
        } else if (key.equals(usedDiskBytesKey)) {
          return Long.toString(diskSpace.getV3()).getBytes();
        } else if (key.equals(diskBytesKey)) {
          return (diskSpace.getV1() + "\t" + diskSpace.getV3() + "\t" + diskSpace.getV2()).getBytes();
        } else if (key.equals(allReplicasKey)) {
          return getAllReplicas();
        } else if (key.equals(allReplicasFreeDiskBytesKey)) {
          return getAllReplicasFreeDiskBytes();
        } else if (key.equals(allReplicasFreeSystemDiskBytesEstimateKey)) {
          return getAllReplicasFreeSystemDiskBytesEstimate();
        } else if (key.equals(exclusionSetKey)) {
          return getExclusionSet();
        } else if (key.equals(ringHealthKey)) {
          return getRingHealth();
        } else {
          throw new RuntimeException("panic");
        }
      }
      return value;
    } catch (KeeperException ke) {
      Log.logErrorWarning(ke);
      return null;
    }
  }

  @Override
  public Map<String, Map<String, String>> getAllStats() {
    Map<String, Map<String, String>> results = new HashMap<>();
    String namespace = getNamespaceProperties().getName();
    if (namespace == null)
      namespace = Namespace.systemName;
    try {
      Map<String, String> thisNsResults = new HashMap<>();
      Triple<Long, Long, Long> diskSpace = getDiskSpace();
      thisNsResults.put(NamespaceStoreConstants.SystemConstants.totalDiskBytesVar, Long.toString(diskSpace.getV1()));
      thisNsResults.put(NamespaceStoreConstants.SystemConstants.freeDiskBytesVar, Long.toString(diskSpace.getV2()));
      thisNsResults.put(NamespaceStoreConstants.SystemConstants.usedDiskBytesVar, Long.toString(diskSpace.getV3()));
      String exclusionSet = StringUtil.toString(ringMaster.getCurrentExclusionSet().getServers(), ',');
      thisNsResults.put(NamespaceStoreConstants.SystemConstants.exclusionSetVar,
          exclusionSet.isEmpty() ? exclusionSet : exclusionSet.substring(0, exclusionSet.length() - 1));
      // ringMaster.getRingHealth() is null initially. See RingMapState2.getRingHealth().
      RingHealth ringHealth = ringMaster.getRingHealth();
      thisNsResults.put(NamespaceStoreConstants.SystemConstants.ringHealthVar,
          (ringHealth == null) ? "null" : ringHealth.toString());
      results.put(namespace, thisNsResults);
    } catch (KeeperException kex) {
      Log.logErrorWarning(kex);
    }
    return results;
  }
}

package com.ms.silverking.cloud.dht.meta;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.ms.silverking.cloud.dht.daemon.RingHealth;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.Triple;

/**
 * Write/Reads health for a particular instance of a particular ring for a particular DHT.
 */
public class RingHealthZK {
  private final MetaClient mc;
  private final String ringName;
  private final long ringConfigVersion;
  private final long configInstanceVersion;

  /*
   * Path format:s
   * <DHTPath>/ringHealth/<RingName>/<RingConfigVersion>/<ConfigInstanceVersion>
   */

  public RingHealthZK(MetaClient mc, String ringName, long ringConfigVersion, long configInstanceVersion)
      throws KeeperException {
    this.mc = mc;
    this.ringName = ringName;
    this.ringConfigVersion = ringConfigVersion;
    this.configInstanceVersion = configInstanceVersion;
    ensureBasePathExists();
  }

  public RingHealthZK(MetaClient mc, Triple<String, Long, Long> ringNameAndVersionPair) throws KeeperException {
    this(mc, ringNameAndVersionPair.getV1(), ringNameAndVersionPair.getV2(), ringNameAndVersionPair.getV3());
  }

  private void ensureBasePathExists() throws KeeperException {
    mc.ensureMetaPathsExist();
    mc.ensurePathExists(getRingHealthPathBase(), true);
  }

  private String getDHTRingHealthPath() {
    return mc.getMetaPaths().getInstanceRingHealthPath();
  }

  private String getRingHealthPathBase() {
    return getDHTRingHealthPath() + "/" + ringName;
  }

  private String getRingConfigVersionPath() {
    return getRingHealthPathBase() + "/" + ZooKeeperExtended.padVersion(ringConfigVersion);
  }

  public String getRingInstanceHealthPath() {
    return getRingConfigVersionPath() + "/" + ZooKeeperExtended.padVersion(configInstanceVersion);
  }

  public void writeHealth(RingHealth health) throws KeeperException {
    String path;

    path = getRingInstanceHealthPath();
    if (!mc.getZooKeeper().exists(path)) {
      mc.getZooKeeper().createAllNodes(path);
    }
    mc.getZooKeeper().setString(path, health.toString());
  }

  public RingHealth readHealth() throws KeeperException {
    try {
      if (mc.getZooKeeper().exists(getRingInstanceHealthPath())) {
        return RingHealth.valueOf(mc.getZooKeeper().getString(getRingInstanceHealthPath()));
      } else {
        return null;
      }
    } catch (NoNodeException nne) {
      return null;
    }
  }
}

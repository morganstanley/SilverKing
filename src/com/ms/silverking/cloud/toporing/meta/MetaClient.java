package com.ms.silverking.cloud.toporing.meta;

import java.io.IOException;

import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import org.apache.zookeeper.CreateMode;

public class MetaClient extends MetaClientBase<MetaPaths> {
  private CloudConfiguration cloudConfiguration;

  private MetaClient(MetaPaths mp, CloudConfiguration cloudConfiguration, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    super(mp, zkConfig);
    this.cloudConfiguration = cloudConfiguration;
  }

  public MetaClient(NamedRingConfiguration ringConfig, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    this(new MetaPaths(ringConfig), ringConfig.getRingConfiguration().getCloudConfiguration(), zkConfig);
  }

  public static MetaClient createMetaClient(String ringName, long ringVersion, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    MetaClient _mc;
    NamedRingConfiguration ringConfig;

    _mc = new MetaClient(new NamedRingConfiguration(ringName, RingConfiguration.emptyTemplate), zkConfig);
    ringConfig = new NamedRingConfiguration(ringName, new RingConfigurationZK(_mc).readFromZK(ringVersion, null));
    return new com.ms.silverking.cloud.toporing.meta.MetaClient(ringConfig, zkConfig);
  }

  public com.ms.silverking.cloud.meta.MetaClient createCloudMC() throws KeeperException, IOException {
    return new com.ms.silverking.cloud.meta.MetaClient(cloudConfiguration, getZooKeeper().getZKConfig());
  }

  public String createConfigInstancePath(long configVersion) throws KeeperException {
    String path;

    path = metaPaths.getConfigInstancePath(configVersion);
    getZooKeeper().createAllNodes(path);
    return getZooKeeper().createString(path + "/", "", CreateMode.PERSISTENT_SEQUENTIAL);
  }

  public String getLatestConfigInstancePath(long configVersion) throws KeeperException {
    String path;
    long latestVersion;

    path = metaPaths.getConfigInstancePath(configVersion);
    getZooKeeper().createAllNodes(path);
    latestVersion = getZooKeeper().getLatestVersion(path);
    if (latestVersion >= 0) {
      return path + "/" + SilverKingZooKeeperClient.padVersion(latestVersion);
    } else {
      return null;
    }
  }

  public long getLatestConfigInstanceVersion(long configVersion) throws KeeperException {
    String path;
    long latestVersion;

    path = metaPaths.getConfigInstancePath(configVersion);
    getZooKeeper().createAllNodes(path);
    latestVersion = getZooKeeper().getLatestVersion(path);
    return latestVersion;
  }
}

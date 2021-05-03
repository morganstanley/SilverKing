package com.ms.silverking.cloud.skfs.meta;

import java.io.IOException;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;

public class MetaClient extends MetaClientBase<MetaPaths> {
  private final String skfsConfigName;

  public MetaClient(String skfsConfigName, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    super(new MetaPaths(skfsConfigName), zkConfig);
    this.skfsConfigName = skfsConfigName;
  }

  public MetaClient(SKGridConfiguration skGridConfig) throws IOException, KeeperException {
    this(skGridConfig.getSKFSConfigName(), skGridConfig.getClientDHTConfiguration().getZKConfig());
  }

  public String getSKFSConfigName() {
    return skfsConfigName;
  }

  public String getSKFSConfig() throws KeeperException {
    String def;
    String latestPath;
    SilverKingZooKeeperClient zk;

    zk = getZooKeeper();
    latestPath = zk.getLatestVersionPath(getMetaPaths().getConfigPath());
    def = zk.getString(latestPath);
    return def;
  }
}

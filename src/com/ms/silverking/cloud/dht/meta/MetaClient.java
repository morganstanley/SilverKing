package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;

public class MetaClient extends MetaClientBase<MetaPaths> {
  private final String dhtName;

  public MetaClient(String dhtName, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    this(new NamedDHTConfiguration(dhtName, null), zkConfig);
  }

  public MetaClient(NamedDHTConfiguration dhtConfig, ZooKeeperConfig zkConfig)
      throws IOException, KeeperException {
    super(new MetaPaths(dhtConfig), zkConfig);
    this.dhtName = dhtConfig.getDHTName();
  }

  public MetaClient(ClientDHTConfiguration clientDHTConfig) throws IOException, KeeperException {
    this(clientDHTConfig.getName(), clientDHTConfig.getZKConfig());
  }

  public MetaClient(SKGridConfiguration skGridConfig) throws IOException, KeeperException {
    this(skGridConfig.getClientDHTConfiguration());
  }

  public String getDHTName() {
    return dhtName;
  }

  public DHTConfiguration getDHTConfiguration() throws KeeperException {
    String def;
    long version;
    String latestPath;
    long zkid;
    ZooKeeperExtended zk;

    zk = getZooKeeper();
    latestPath = zk.getLatestVersionPath(getMetaPaths().getInstanceConfigPath());
    version = zk.getLatestVersionFromPath(latestPath);
    def = zk.getString(latestPath);
    zkid = zk.getStat(latestPath).getMzxid();
    return DHTConfiguration.parse(def, version, zkid);
  }

  public IpAliasConfiguration getIpAliasConfiguration(String ipAliasMapName) throws KeeperException {
    if (ipAliasMapName != null) {
      String fullPath = MetaPaths.getIpAliasMapPath(ipAliasMapName);
      Log.finef("ipAliasMapName %s", ipAliasMapName);
      Log.finef("fullPath %s", fullPath);
      ZooKeeperExtended zk = getZooKeeper();
      if (zk.exists(MetaPaths.ipAliasesBase) && zk.exists(fullPath)) {
        String latestPath = zk.getLatestVersionPath(fullPath);
        long version = zk.getLatestVersionFromPath(latestPath);
        String def = zk.getString(latestPath);
        Log.finef("def %s", def);
        return IpAliasConfiguration.parse(def, version);
      } else {
        Log.finef("emptyTemplate 1");
        return IpAliasConfiguration.emptyTemplate;
      }
    } else {
      Log.finef("emptyTemplate 2");
      return IpAliasConfiguration.emptyTemplate;
    }
  }
}

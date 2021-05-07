package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;

import com.ms.silverking.cloud.dht.util.LocalZkServerTestSuite;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

public class IpAliasConfigurationZkTest extends LocalZkServerTestSuite implements Watcher {
  public IpAliasConfigurationZkTest() {
  }

  @Test
  public void test() throws KeeperException, IOException {
    ZooKeeperConfig zkConfig;
    SilverKingZooKeeperClient zk;
    IpAliasConfiguration ipAliasConfiguration;
    IpAliasConfigurationZk ipAliasConfigurationZk;
    MetaToolOptions options;

    zkConfig = getZkConfig();
    zk = new SilverKingZooKeeperClient(zkConfig, 1000);
    String def = "ipAliasMap={192.168.0.1=10.102.102.10:7777,192.168.0.2=10.102.102.11:7777,192.168.0.3=10.102" +
        ".102.10:6614}";
    ipAliasConfiguration = IpAliasConfiguration.parse(def, 0);
    try {
      ipAliasConfigurationZk = new IpAliasConfigurationZk(new MetaClient("IpAliasConfigurationZkTest", zk.getZKConfig()));
    } catch (org.apache.zookeeper.KeeperException e) {
      throw new RuntimeException(e);
    }
    options = new MetaToolOptions();
    options.name = "IpAliasConfigurationZkTest";
    try {
      ipAliasConfigurationZk.writeToZK(ipAliasConfiguration, options);
    } catch (org.apache.zookeeper.KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(WatchedEvent event) {
  }
}

package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;

import com.ms.silverking.cloud.dht.util.LocalZkServerTestSuite;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

public class IpAliasConfigurationZkTest extends LocalZkServerTestSuite implements Watcher {
  public IpAliasConfigurationZkTest() {
  }

  @Test
  public void test() throws KeeperException, IOException {
    ZooKeeperConfig zkConfig;
    ZooKeeperExtended zk;
    IpAliasConfiguration ipAliasConfiguration;
    IpAliasConfigurationZk ipAliasConfigurationZk;
    MetaToolOptions options;

    zkConfig = getZkConfig();
    zk = new ZooKeeperExtended(zkConfig, 1000, this);
    String def = "ipAliasMap={192.168.0.1=10.102.102.10:7777,192.168.0.2=10.102.102.11:7777,192.168.0.3=10.102" +
        ".102.10:6614}";
    ipAliasConfiguration = IpAliasConfiguration.parse(def, 0);
    ipAliasConfigurationZk = new IpAliasConfigurationZk(new MetaClient("IpAliasConfigurationZkTest", zk.getZKConfig()));
    options = new MetaToolOptions();
    options.name = "IpAliasConfigurationZkTest";
    ipAliasConfigurationZk.writeToZK(ipAliasConfiguration, options);
  }

  @Override
  public void process(WatchedEvent event) {
  }
}

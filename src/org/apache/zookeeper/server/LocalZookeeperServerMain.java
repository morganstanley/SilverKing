package org.apache.zookeeper.server;

public class LocalZookeeperServerMain extends ZooKeeperServerMain {

  public void shutdownZk() {
    this.shutdown();
  }
}

package com.ms.silverking.cloud.dht.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import com.ms.silverking.cloud.zookeeper.LocalZKImpl;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.log.Log;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class LocalZkServerTestSuite {
  private static Path tempDir;
  private static int zkPort;
  private static ZooKeeperConfig zkConfig;

  @BeforeClass
  public static void setUp() throws IOException {
    tempDir = Files.createTempDirectory(null);
    File zkDir = new File(tempDir.toFile(), "zookeeper");
    if (!zkDir.mkdirs()) {
      throw new IllegalStateException("Could not create local ZK dirs: " + zkDir);
    }
    zkPort = LocalZKImpl.startLocalZK(zkDir.getAbsolutePath());
    zkConfig = new ZooKeeperConfig(InetAddress.getLoopbackAddress().getHostAddress() + ":" + zkPort);
    Log.warning("Embedded ZooKeeper running at: " + zkConfig);
  }

  @AfterClass
  public static void stop() {
    Log.warning("Stopping all global DHTNode processes");
    LocalZKImpl.shutdown();
    try {
      FileUtils.deleteDirectory(tempDir.toFile());
    } catch (IOException e) {
      Log.logErrorWarning(e);
    }
  }

  protected static ZooKeeperConfig getZkConfig() {
    return zkConfig;
  }

  protected static int getZkPort() {
    return zkPort;
  }
}
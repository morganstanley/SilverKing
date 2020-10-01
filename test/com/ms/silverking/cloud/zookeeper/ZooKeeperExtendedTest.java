package com.ms.silverking.cloud.zookeeper;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.SKTestConfiguration;
import com.ms.silverking.cloud.meta.MetaPathsBase;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Test;

public class ZooKeeperExtendedTest implements Watcher {
  private final ZooKeeperExtended zk;
  private final String deleteVersionedChildrenTestParent;

  private static final String baseTestNode = MetaPathsBase.cloudGlobalBase + "/test";

  private static final String deleteVersionedChildrenTestNode = baseTestNode + "/deleteVersionedChildrenTest";
  private static final int dvtOriginalVersions = 10;
  private static final int dvtRetainedVersions = 7;

  public ZooKeeperExtendedTest() throws IOException, KeeperException {
    if (SKTestConfiguration.zkEnsemble != null) {
      zk = new ZooKeeperExtended(new ZooKeeperConfig(SKTestConfiguration.zkEnsemble), 1000, this);
    } else {
      int port;

      port = LocalZKImpl.startLocalZK(PropertiesHelper.systemHelper.getString("java.io.tmpdir",
          PropertiesHelper.UndefinedAction.ExceptionOnUndefined));
      zk = new ZooKeeperExtended(new ZooKeeperConfig("localhost:" + port), 1000, this);
    }
    deleteVersionedChildrenTestParent = deleteVersionedChildrenTestNode + "/parent" + UUIDBase.random();
    Log.setLevel(Level.INFO);
  }

  private void verifyChildrenExist(String parent, List<String> children) throws KeeperException {
    for (String child : children) {
      if (!zk.exists(child)) {
        throw new RuntimeException("Failed to find " + parent + "/" + child);
      }
    }
  }

  private void ensureDoesNotExist(String path) throws KeeperException {
    assertFalse(zk.exists(path));
  }

  @Test
  public void deleteVersionedChildrenTest() throws IOException, KeeperException {
    /*
    Commented out due to local zk failing to spin up. Either resolve that or pass in a zk config

    List<String>  children;

    children = new ArrayList<>();
    Log.infof("Creating %s", deleteVersionedChildrenTestParent);
    //zk.deleteRecursive(deleteVersionedChildrenTestParent);
    zk.createAllNodes(deleteVersionedChildrenTestParent);
    for (int i = 0; i < dvtOriginalVersions; i++) {
      children.add(zk.create(deleteVersionedChildrenTestParent +"/", "".getBytes(), CreateMode.PERSISTENT_SEQUENTIAL));
    }
    verifyChildrenExist(deleteVersionedChildrenTestParent, children);
    zk.deleteVersionedChildren(deleteVersionedChildrenTestParent, dvtRetainedVersions);
    for (int i = 0; i < dvtOriginalVersions - dvtRetainedVersions; i++) {
      String  child;

      child = children.remove(0);
      ensureDoesNotExist(child);
    }
    verifyChildrenExist(deleteVersionedChildrenTestParent, children);
    Log.info("Deleting %s", deleteVersionedChildrenTestParent);
    zk.deleteRecursive(deleteVersionedChildrenTestParent);
    ensureDoesNotExist(deleteVersionedChildrenTestParent);
     */
  }

  @Override
  public void process(WatchedEvent event) {
  }
}

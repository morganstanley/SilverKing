package com.ms.silverking.cloud.meta;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.log.Log;

public class MetaClientBase<T extends MetaPathsBase> extends MetaClientCore {
  protected final T metaPaths;

  public MetaClientBase(T metaPaths, ZooKeeperConfig zkConfig, Watcher watcher) throws IOException, KeeperException {
    super(zkConfig, watcher);
    this.metaPaths = metaPaths;
  }

  public MetaClientBase(T paths, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    this(paths, zkConfig, null);
  }

  public T getMetaPaths() {
    return metaPaths;
  }

  public void ensureMetaPathsExist() throws KeeperException {
    try {
      getZooKeeper().createAllNodes(metaPaths.getPathList());
    } catch (RuntimeException re) {
      Log.warningf("Failed to create meta paths %s", metaPaths.getPathList());
      throw re;
    }
  }

  public void ensurePathExists(String path, boolean createIfMissing) throws KeeperException {
    if (!getZooKeeper().exists(path)) {
      if (createIfMissing) {
        try {
          getZooKeeper().create(path);
        } catch (KeeperException ke) {
          if (!getZooKeeper().exists(path)) {
            throw new RuntimeException("Path doesn't exist and creation failed: " + path);
          }
        }
      } else {
        throw new RuntimeException("Path doesn't exist: " + path);
      }
    }
  }

  public void ensurePathDoesNotExist(String path) throws KeeperException {
    if (getZooKeeper().exists(path)) {
      throw new RuntimeException("Path exists: " + path);
    }
  }
}

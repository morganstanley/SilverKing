package com.ms.silverking.cloud.meta;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;

public class MetaClientCore {
  protected final ZooKeeperConfig zkConfig;
  private ZooKeeperExtended zk; //only used if not shareZK

  private static final int sessionTimeout;

  static {
    sessionTimeout = PropertiesHelper.systemHelper.getInt(DHTConstants.zookeeperSessionTimeoutProperty, 4 * 60 * 1000);
    Log.warningf("%s %d", DHTConstants.zookeeperSessionTimeoutProperty, sessionTimeout);
  }

  private static final int connectAttempts = 4;
  private static final double connectionLossSleepSeconds = 5.0;

  private static final int defaultGetZKSleepUnit = 8;
  private static final int defaultGetZKMaxAttempts = 15;

  private static final boolean shareZK = true;
  protected static final ConcurrentMap<ZooKeeperConfig, Lock> lockMap;
  protected static final ConcurrentMap<ZooKeeperConfig, ZooKeeperExtended> zkMap;

  static {
    if (shareZK) {
      lockMap = new ConcurrentHashMap<>();
      zkMap = new ConcurrentHashMap<>();
    } else {
      zkMap = null;
      lockMap = null;
    }
  }

  private Lock acquireLockIfShared(ZooKeeperConfig zkConfig) {
    Lock lock;

    if (shareZK) {
      lock = lockMap.get(zkConfig);
      if (lock == null) {
        lockMap.putIfAbsent(zkConfig, new ReentrantLock());
        lock = lockMap.get(zkConfig);
      }
      lock.lock();
    } else {
      lock = null;
    }
    return lock;
  }

  private void releaseLockIfShared(Lock lock) {
    if (lock != null) {
      lock.unlock();
    }
  }

  private void setZK(ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    Lock lock;
    ZooKeeperExtended _zk;

    lock = acquireLockIfShared(zkConfig);
    try {
      if (shareZK) {
        _zk = zkMap.get(zkConfig);
      } else {
        _zk = null;
      }
      if (_zk == null) {
        Log.info(String.format("Getting ZooKeeperExtended for %s\n", zkConfig));
          zk = new ZooKeeperExtended(zkConfig, sessionTimeout, null);
        Log.info(String.format("Done getting ZooKeeperExtended for %s\n", zkConfig));
        if (shareZK) {
          zkMap.putIfAbsent(zkConfig, zk);
        }
      } else {
        zk = _zk;
      }
    } finally {
      releaseLockIfShared(lock);
    }
  }

  public MetaClientCore(ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    this.zkConfig = zkConfig;
    setZK(zkConfig);
  }

  public ZooKeeperExtended _getZooKeeper() {
    if (shareZK) {
      return zkMap.get(zkConfig);
    } else {
      return zk;
    }
  }

  public ZooKeeperExtended getZooKeeper(int getZKMaxAttempts, int getZKSleepUnit) throws KeeperException {
    ZooKeeperExtended _zk;
    int attemptIndex;

    assert getZKMaxAttempts > 0;
    assert getZKSleepUnit > 0;
    _zk = null;
    attemptIndex = 0;
    while (_zk == null) {
      _zk = _getZooKeeper();
      if (_zk == null) {
        if (attemptIndex < getZKMaxAttempts - 1) {
          ThreadUtil.randomSleep(getZKSleepUnit << attemptIndex);
          ++attemptIndex;
        } else {
          Log.warning("getZooKeeper() failed after " + (attemptIndex + 1) + " attempts");
          //throw KeeperException.forMethod("getZooKeeper", new OperationTimeoutException());
        }
      }
    }
    return _zk;
  }

  public ZooKeeperExtended getZooKeeper() throws KeeperException {
    return getZooKeeper(defaultGetZKMaxAttempts, defaultGetZKSleepUnit);
  }

  public void closeZkExtendeed() {
    _getZooKeeper().close();
  }

  public static void clearZkMap() {
    for (ZooKeeperExtended zk : zkMap.values()) {
      zk.close();
    }
    zkMap.clear();
  }

  public void close() {
    //zk.close(); // FIXME
  }
}

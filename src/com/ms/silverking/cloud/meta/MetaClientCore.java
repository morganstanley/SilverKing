package com.ms.silverking.cloud.meta;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.OperationTimeoutException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper.States;

import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

public class MetaClientCore implements Watcher {
    protected final ZooKeeperConfig zkConfig;
    private ZooKeeperExtended       zk; //only used if not shareZK
    private final Watcher           watcher;
    
    private static final int    sessionTimeout = 4 * 60 * 1000;
    private static final int    connectAttempts = 4;
    private static final double connectionLossSleepSeconds = 5.0;
    
    private static final int	defaultGetZKSleepUnit = 8;
    private static final int	defaultGetZKMaxAttempts = 15;
    
    private static final boolean	shareZK = true;
    protected static final ConcurrentMap<ZooKeeperConfig,Lock>				lockMap;
    protected static final ConcurrentMap<ZooKeeperConfig,ZooKeeperExtended>	zkMap;
    
    static {
    	if (shareZK) {
    		lockMap = new ConcurrentHashMap<>();
    		zkMap = new ConcurrentHashMap<>();
    	} else {
    		zkMap = null;
    		lockMap = null;
    	}
    }
    
    public MetaClientCore(ZooKeeperConfig zkConfig, Watcher watcher) throws IOException, KeeperException {
        this.watcher = watcher;
        this.zkConfig = zkConfig;
    	setZK(zkConfig);
    }
    
    private Lock acquireLockIfShared(ZooKeeperConfig zkConfig) {
        Lock    lock;
        
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
        Lock                lock;
        ZooKeeperExtended   _zk;

        lock = acquireLockIfShared(zkConfig);
        try {
            if (shareZK) {      
                _zk = zkMap.get(zkConfig);
            } else {
                _zk = null;
            }
            if (_zk == null) {
                Log.info(String.format("Getting ZooKeeperExtended for %s\n", zkConfig));
                zk = ZooKeeperExtended.getZooKeeperWithRetries(zkConfig, sessionTimeout, this, connectAttempts);
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
        this(zkConfig, null);
    }
        
    public ZooKeeperExtended _getZooKeeper() {
        if (shareZK) {
            return zkMap.get(zkConfig);
        } else {
            return zk;
        }
    }
    
    public ZooKeeperExtended getZooKeeper(int getZKMaxAttempts, int getZKSleepUnit) throws KeeperException {
    	ZooKeeperExtended	_zk;
    	int					attemptIndex;

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
    				Log.warning("getZooKeeper() failed after "+ (attemptIndex + 1) +" attempts");
    				throw new OperationTimeoutException();
    			}
    		}
    	}
		return _zk;
    }
    
    public ZooKeeperExtended getZooKeeper() throws KeeperException {
    	return getZooKeeper(defaultGetZKMaxAttempts, defaultGetZKSleepUnit);
    }
    
    private void handleSessionExpiration() {
        Lock    lock;
        
        lock = acquireLockIfShared(zkConfig);
        
        try {
            boolean             established;
            
            established = false;        
            while (!established) {
                ZooKeeperExtended   _zk;
                
                ThreadUtil.sleepSeconds(connectionLossSleepSeconds);
                _zk = zkMap.get(zkConfig);
                if (_zk != null && _zk.getState() != States.CLOSED) {
                    established = true;
                } else {
                    zkMap.remove(zkConfig);
                    try {
                        Log.warning(String.format("Attempting to reestablish session %s\n", zkConfig));
                        zk = ZooKeeperExtended.getZooKeeperWithRetries(zkConfig, sessionTimeout, this, connectAttempts);
                        Log.warning(String.format("Session restablished %s\n", zkConfig));
                        established = true;
                        if (shareZK) {
                            zkMap.put(zkConfig, zk);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            releaseLockIfShared(lock);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        ZooKeeperExtended   _zk;
        
        _zk = _getZooKeeper(); 
        if (_zk == null || _zk.getState() == States.CLOSED) {
            handleSessionExpiration();
        }
        //Log.warning(event.toString());
        synchronized (this) {
            this.notifyAll();
        }
        if (watcher != null) {
            watcher.process(event);
        }
    }
    
    public void close() {
        //zk.close(); // FIXME
    }
}

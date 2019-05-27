package com.ms.silverking.cloud.meta;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;

/**
 * Watchers all children under a znode for changes.
 */
public class ChildrenWatcher extends WatcherBase {
    private final ChildrenListener   listener;    
    private volatile Map<String,byte[]>   childStates;
    
    public ChildrenWatcher(Timer timer, MetaClientCore metaClientCore, String basePath, ChildrenListener listener, 
                          long intervalMillis, long maxInitialSleep) {
        super(metaClientCore, timer, basePath, intervalMillis, maxInitialSleep);
        this.listener = listener;
    }
    
    public ChildrenWatcher(MetaClientCore metaClientCore, String basePath, ChildrenListener listener, 
            long intervalMillis, long maxInitialSleep) {
        this(null, metaClientCore, basePath, listener, intervalMillis, maxInitialSleep);
    }
    
    public ChildrenWatcher(Timer timer, MetaClientCore metaClientCore, String basePath, ChildrenListener listener, 
                          long intervalMillis) {
        this(timer, metaClientCore, basePath, listener, intervalMillis, intervalMillis);
    }
    
    public ChildrenWatcher(MetaClientCore metaClientCore, String basePath, ChildrenListener listener, 
            long intervalMillis) {
        this(metaClientCore, basePath, listener, intervalMillis, intervalMillis);
    }
    
    private Map<String,byte[]> readChildStates() throws KeeperException {
        ZooKeeperExtended   _zk;
        List<String>    children;
        
        _zk = metaClientCore.getZooKeeper();
        children = _zk.getChildren(basePath, this);        
        return _zk.getByteArrays(basePath, ImmutableSet.copyOf(children), this, this);
    }
    
    protected void _doCheck() throws KeeperException {
        Map<String,byte[]>  latestChildStates;
        
        latestChildStates = readChildStates();
        if (!mapsAreEqual(latestChildStates, childStates)) {
            childStates = latestChildStates;
            listener.childrenChanged(basePath, latestChildStates);
        }
    }

    private void checkChildStates() {
    	try {
			doCheck();
		} catch (KeeperException ke) {
			throw new RuntimeException(ke);
		}
    }
    
    /**
     * Can't use Guava Maps.difference() since we have byte arrays here.
     * @param latestChildStates
     * @param childStates2
     * @return
     */
    private boolean mapsAreEqual(Map<String, byte[]> m1, Map<String, byte[]> m2) {
        Set<String> m1KeySet;
        
        m1KeySet = m1.keySet();
        if (m2 != null && m1KeySet.equals(m2.keySet())) {
            for (String key : m1KeySet) {
                if (!Arrays.equals(m1.get(key), m2.get(key))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public void connected(WatchedEvent event) {
    	Log.fine("connected");
        checkChildStates();
    }
    
    public void nodeCreated(WatchedEvent event) {
    	Log.fine("nodeCreated");
        checkChildStates();
    }
    
    public void nodeDeleted(WatchedEvent event) {
    	Log.warning("nodeDeleted ", event.getPath());
        checkChildStates();
    }
    
    public void nodeDataChanged(WatchedEvent event) {
    	Log.fine("nodeDataChanged ");
    	checkChildStates();
    }
    
    public void nodeChildrenChanged(WatchedEvent event) {
    	Log.fine("nodeChildrenChanged");
        checkChildStates();
    }        
}

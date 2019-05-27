package com.ms.silverking.cloud.meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;


/**
 * Watches a single versioned ZooKeeper path for new versions.
 */
public class VersionWatcher extends WatcherBase {
    private final VersionListener   listener;
    private long lastNotifiedVersion;
    
    private static final boolean    verbose = false;
    
    public VersionWatcher(MetaClientCore metaClientCore, String basePath, VersionListener listener, 
                          long intervalMillis, long maxInitialSleep) {
        super(metaClientCore, basePath, intervalMillis, maxInitialSleep);
        this.listener = listener;
        lastNotifiedVersion = Long.MIN_VALUE;
    }
    
    public VersionWatcher(MetaClientCore metaClientCore, String basePath, VersionListener listener, 
                          long intervalMillis) {
        this(metaClientCore, basePath, listener, intervalMillis, intervalMillis);
    }
    
    protected void _doCheck() throws KeeperException {
		Log.fine("checkVersions");
    	try {
            ZooKeeperExtended   _zk;
            List<String>    children;
            List<Long>      currentVersions;
            long            mostRecentVersion;
            
            if (verbose) {
                Log.warning("VersionCheck start: ", basePath);
            }
            _zk = metaClientCore.getZooKeeper();
            children = _zk.getChildren(basePath, this);
            currentVersions = new ArrayList<>(children.size());
            for (String child : children) {
                currentVersions.add(Long.parseLong(child));
            }
            Collections.sort(currentVersions);
            if (currentVersions.size() > 0) {
                mostRecentVersion = currentVersions.get(currentVersions.size() - 1);
            } else {
                mostRecentVersion = Long.MIN_VALUE;
            }
            if (active && mostRecentVersion > lastNotifiedVersion) {
                lastNotifiedVersion = mostRecentVersion;
                listener.newVersion(basePath, mostRecentVersion);
            }
            if (verbose) {
                Log.warning("VersionCheck complete: ", basePath);
            }
        } catch (KeeperException ke) {
    		System.out.println("*** ZooKeeper state: "+ metaClientCore.getZooKeeper().getState());
    		throw ke;
        }
    }

	private void checkVersions() {
		try {
			doCheck();
		} catch (KeeperException re) {
			throw new RuntimeException(re);
		}
	}
    
    public void connected(WatchedEvent event) {
    	Log.fine("connected");
    	checkVersions();
    }
    
    public void nodeCreated(WatchedEvent event) {
    	Log.fine("nodeCreated");
    	checkVersions();
    }
    
    public void nodeDeleted(WatchedEvent event) {
    	Log.warning("nodeDeleted ", event.getPath());
    	checkVersions();
    }
    
    public void nodeChildrenChanged(WatchedEvent event) {
    	Log.fine("nodeChildrenChanged");
    	checkVersions();
    }    
}

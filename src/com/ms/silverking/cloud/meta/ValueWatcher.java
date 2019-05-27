package com.ms.silverking.cloud.meta;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;


/**
 * Watches a single versioned ZooKeeper path for new versions.
 */
public class ValueWatcher extends WatcherBase {
    private final ValueListener   listener;
    private long lastNotifiedZXID;
    
    private static final boolean    verbose = false;
    
    public ValueWatcher(MetaClientCore metaClientCore, String basePath, ValueListener listener, 
                          long intervalMillis, long maxInitialSleep) {
        super(metaClientCore, basePath, intervalMillis, maxInitialSleep);
        this.listener = listener;
        lastNotifiedZXID = Long.MIN_VALUE;
    }
    
    public ValueWatcher(MetaClientCore metaClientCore, String basePath, ValueListener listener, 
                          long intervalMillis) {
        this(metaClientCore, basePath, listener, intervalMillis, intervalMillis);
    }
    
    protected void _doCheck() throws KeeperException {
        try {
            ZooKeeperExtended   _zk;
            byte[]              value;
            Stat                stat;
            
            if (verbose) {
                Log.warning("ValueCheck start: ", basePath);
            }
            _zk = metaClientCore.getZooKeeper();
            stat = new Stat();
            try {
                value = _zk.getData(basePath, this, stat);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            
            if (stat.getMzxid() > lastNotifiedZXID) {
                listener.newValue(basePath, value, stat);
                lastNotifiedZXID = stat.getMzxid();
            }
            if (verbose) {
                Log.warning("ValueCheck complete: ", basePath);
            }
        } catch (KeeperException ke) {
            System.out.println("*** ZooKeeper state: "+ metaClientCore.getZooKeeper().getState());
            throw ke;
        }
    }
    
    private void checkValue() {
    	try {
			doCheck();
		} catch (KeeperException ke) {
			throw new RuntimeException(ke);
		}
    }
    
    public void nodeDataChanged(WatchedEvent event) {
    	Log.fine("nodeDataChanged");
    	checkValue();
    }    

    public void connected(WatchedEvent event) {
    	Log.fine("connected");
    	checkValue();
    }
    
    public void nodeCreated(WatchedEvent event) {
    	Log.fine("nodeCreated");
    	checkValue();
    }
    
    public void nodeDeleted(WatchedEvent event) {
    	Log.warning("Unexpected nodeDeleted ", event.getPath());
    }
    
    public void nodeChildrenChanged(WatchedEvent event) {
    	Log.fine("nodeChildrenChanged");
    	//checkValue();
    }            
}

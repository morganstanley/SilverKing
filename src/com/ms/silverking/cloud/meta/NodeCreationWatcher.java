package com.ms.silverking.cloud.meta;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

public class NodeCreationWatcher implements Watcher {
    private final ZooKeeperExtended         zk;
    private final String                    parent;
    private final String                    path;
    private final NodeCreationListener      listener;
    
    private static final int    errorSleepMinMillis = 100;
    private static final int    errorSleepMaxMillis = 30 * 1000;
    
    private static final boolean    debug = false;
    
    public NodeCreationWatcher(ZooKeeperExtended zk, String path, NodeCreationListener listener) {
        this.zk = zk;
        this.path = path;
        this.listener = listener;
        parent = ZooKeeperExtended.parentPath(path);
        if (!checkForExistence()) {
            setWatch();
        }
    }
    
    private void setWatch() {
        boolean set;
        
        set = false;
        while (!set) {
            try {
                if (debug) {
                    Log.warning("Setting watch on: "+ parent);
                }
                zk.getChildren(parent, this);
                set = true;
            } catch (Exception e) {
                Log.logErrorWarning(e);
            }
            if (!set) {
                Log.warning("Sleeping for retry...");
                ThreadUtil.randomSleep(errorSleepMinMillis, errorSleepMaxMillis);
                Log.warning("Retrying...");
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        boolean connected;
        boolean exists;
        
        if (debug) {
            Log.warning(event);
        }
        switch (event.getState()) {
        case SaslAuthenticated:
        case SyncConnected:
            connected = true;
            break;
        default:
            connected = false;
        }
        if (connected) {
            switch (event.getType()) {
            case NodeChildrenChanged:
                exists = checkForExistence();
                if (!exists) {
                    setWatch();
                }
                break;
            default:
                break;
            }
        } else {
            setWatch();
        }
    }

    private boolean checkForExistence() {
        try {
            if (zk.exists(path)) {
                listener.nodeCreated(path);
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            Log.logErrorWarning(e);
            return false;
        }
    }
}

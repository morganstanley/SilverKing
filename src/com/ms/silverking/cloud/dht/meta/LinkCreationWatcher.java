package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.meta.NodeCreationListener;
import com.ms.silverking.cloud.meta.NodeCreationWatcher;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

public class LinkCreationWatcher implements NodeCreationListener {
    private final long  child;
    private final LinkCreationListener  listener;
    private final String    path;
    private final ZooKeeperExtended zk;
    
    private static final boolean    debug = false;
    
    public LinkCreationWatcher(ZooKeeperExtended zk, String basePath, long child, LinkCreationListener listener) {
        this.zk = zk;
        path = basePath +"/"+ Long.toHexString(child);
        this.child = child;
        this.listener = listener;
        if (debug) {
            Log.warning("Watching: "+ path);
        }
        new NodeCreationWatcher(zk, path, this);
    }

    @Override
    public void nodeCreated(String path) {
        if (path.lastIndexOf('/') > 0) {
            long    parent;

            try {
                parent = NumConversion.parseHexStringAsUnsignedLong(zk.getString(path));
            } catch (Exception e) {
                Log.logErrorWarning(e, "Unable to read link parent");
                return;
            }
            if (debug) {
                if (child == parent) {
                	Log.warningf("Ignoring circular link %s %x %x", path, child, parent);
                } else {
                	Log.warningf("Calling link created: %s %x %x", path, child, parent);
                }
            }
            listener.linkCreated(child, parent);
        } else {
            Log.warning("Bogus LinkCreationWathcer.nodeCreated() path: "+ path);
        }
    }
}

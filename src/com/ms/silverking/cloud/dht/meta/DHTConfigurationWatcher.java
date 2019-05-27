package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.log.Log;

/**
 * Watches for new DHTConfigurations
 */
public class DHTConfigurationWatcher implements VersionListener {
    private final MetaClient  mc;
    private final MetaPaths   mp;
    private final ZooKeeperConfig   zkConfig;
    private volatile DHTConfiguration   dhtConfig;
    private final boolean   enableLogging;
    private final List<DHTConfigurationListener>    dhtConfigurationListeners;
    
    public DHTConfigurationWatcher(ZooKeeperConfig zkConfig, String dhtName, 
                          long intervalMillis, boolean enableLogging) throws IOException, KeeperException {
        mc = new MetaClient(dhtName, zkConfig);
        mp = mc.getMetaPaths();
        this.zkConfig = zkConfig;
        new VersionWatcher(mc, mp.getInstanceConfigPath(), this, intervalMillis, 0);
        this.enableLogging = enableLogging;
        dhtConfigurationListeners = Collections.synchronizedList(new ArrayList<>());
    }
    
    public DHTConfigurationWatcher(ZooKeeperConfig zkConfig, String dhtName, long intervalMillis) 
                  throws IOException, KeeperException {
        this(zkConfig, dhtName, intervalMillis, true);
    }
    
    public ZooKeeperConfig getZKConfig() {
        return zkConfig;
    }
    
    public MetaClient getMetaClient() {
        return mc;
    }
    
    public void waitForDHTConfiguration() {
        synchronized (this) {
            while (dhtConfig == null) {
                try {
                    this.wait();
                } catch (InterruptedException ie) {
                }
            }
        }
    }
    
    public void addListener(DHTConfigurationListener listener) {
        dhtConfigurationListeners.add(listener);
    }
    
    public DHTConfiguration getDHTConfiguration() {
        return dhtConfig;
    }
    
    /**
     * Called when a new DHT configuration is found. Reads the configuration notifies listeners.
     */
    private void newDHTConfiguration() {
        try {
            synchronized (this) {
            	DHTConfiguration	_dhtConfig;
            	
                _dhtConfig = mc.getDHTConfiguration();
                dhtConfig = _dhtConfig;
                if (enableLogging) {
                    Log.warning("DHTConfigurationWatcher.newDHTConfiguration: "+ dhtConfig);
                }
                if (dhtConfig != null) {
                	this.notifyAll();
                } else {
                	Log.warning("Ignoring null dhtConfig");
                }
                notifyListeners(_dhtConfig);
            }
        } catch (Exception e) {
            Log.logErrorWarning(e);
        }
    }
    
    /*
     * Called when the dht configuration changes.
     */
    @Override
    public void newVersion(String basePath, long version) {
        if (enableLogging) {
            Log.fine("DHTConfigurationWatcher.newVersion: "+ basePath +" "+ version);
        }
        if (basePath.equals(mp.getInstanceConfigPath())) {
            newDHTConfiguration();
        } else {
            Log.warning("Unexpected update in DHTConfigurationWatcher: "+ basePath);
        }
    }
    
    // FUTURE - provide an ordering guarantee
    private void notifyListeners(DHTConfiguration dhtConfiguration) {
        for (DHTConfigurationListener listener : dhtConfigurationListeners) {
            listener.newDHTConfiguration(dhtConfiguration);
        }
    }
}

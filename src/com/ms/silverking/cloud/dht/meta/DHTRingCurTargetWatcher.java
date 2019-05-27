package com.ms.silverking.cloud.dht.meta;

import org.apache.zookeeper.data.Stat;

import com.ms.silverking.cloud.meta.ValueListener;
import com.ms.silverking.cloud.meta.ValueWatcher;
import com.ms.silverking.log.Log;

public class DHTRingCurTargetWatcher implements ValueListener {
    private final String            dhtName;
    private final DHTConfiguration  dhtConfig;
    private final MetaClient		mc;
    private final ValueWatcher      curRingWatcher;
    private ValueWatcher      targetRingWatcher;
    private final DHTRingCurTargetListener  listener;
    
    private static final int    initialIntervalMillis = 10 * 1000;
    private static final int    checkIntervalMillis = 1 * 60 * 1000;
    private static final boolean	debug = true;
    
    public DHTRingCurTargetWatcher(MetaClient mc, String dhtName, DHTConfiguration dhtConfig, 
                                   DHTRingCurTargetListener listener) {
        this.dhtName = dhtName;
        this.dhtConfig = dhtConfig;
        this.mc = mc;
        curRingWatcher = new ValueWatcher(mc, MetaPaths.getInstanceCurRingAndVersionPairPath(dhtName), 
                                          this, checkIntervalMillis, initialIntervalMillis);
        this.listener = listener;
    }
    
    public void startTargetRingWatcher() {
        targetRingWatcher = new ValueWatcher(mc, MetaPaths.getInstanceTargetRingAndVersionPairPath(dhtName), 
                this, checkIntervalMillis, initialIntervalMillis);
    }

    @Override
    public void newValue(String basePath, byte[] value, Stat stat) {
    	if (debug) {
    		Log.warning("DHTRingCurTargetWatcher.newValue: ", basePath);
    	}
    	if (value.length < DHTRingCurTargetZK.minValueLength) {
    		Log.warning("Value length too small. ", basePath +" "+ value.length);
    	} else {
	        if (basePath.equals(MetaPaths.getInstanceCurRingAndVersionPairPath(dhtName))) {
	        	if (debug) {
	        		Log.warning("DHTRingCurTargetWatcher.newCurRingAndVersion: ", basePath);
	        	}
	            listener.newCurRingAndVersion(DHTRingCurTargetZK.bytesToNameAndVersion(value));
	        } else if (basePath.equals(MetaPaths.getInstanceTargetRingAndVersionPairPath(dhtName))) {
	        	if (debug) {
	        		Log.warning("DHTRingCurTargetWatcher.newTargetRingAndVersion: ", basePath);
	        	}
	            listener.newTargetRingAndVersion(DHTRingCurTargetZK.bytesToNameAndVersion(value));
	        } else {
	            Log.warning("Unexpected value update in DHTRingCurTargetWatcher: ", basePath);
	        	Log.warning(MetaPaths.getInstanceCurRingAndVersionPairPath(dhtName));
	        	Log.warning(MetaPaths.getInstanceTargetRingAndVersionPairPath(dhtName));
	        }
    	}
    }
}

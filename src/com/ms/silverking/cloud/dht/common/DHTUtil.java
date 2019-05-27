package com.ms.silverking.cloud.dht.common;

import java.util.Timer;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceServerSideCode;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.util.SafeTimer;

public class DHTUtil {
    private static final Timer  timer = new SafeTimer(true);
    
    static {
        initializeObjectParsers();
    }
    
    private static void initializeObjectParsers() {
        new SecondaryTarget(null, null);
        NamespaceServerSideCode.init();
        NamespaceOptions.init();
    }

    public static long currentTimeMillis() {
    	// FUTURE deprecate this function and replace with direct calls to SystemTimeUtil
    	return SystemTimeUtil.systemTimeSource.absTimeMillis();
    }
    
    public static Timer timer() {
        return timer;
    }
}

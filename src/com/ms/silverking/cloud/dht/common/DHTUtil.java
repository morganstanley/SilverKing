package com.ms.silverking.cloud.dht.common;

import java.util.Timer;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceServerSideCode;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.time.RelNanosAbsMillisTimeSource;
import com.ms.silverking.time.SystemTimeSource;
import com.ms.silverking.util.SafeTimer;

public class DHTUtil {
	
	// In the process of deprecating timeSource in favor of SystemTimeUtil
    private static final RelNanosAbsMillisTimeSource    timeSource;

    private static final Timer  timer = new SafeTimer(true);
    
    static {
        // FUTURE - consider making this more efficient by using timer driven time source
        timeSource = new SystemTimeSource();
        initializeObjectParsers();
    }
    
    private static void initializeObjectParsers() {
        new SecondaryTarget(null, null);
        NamespaceOptions.init();
        NamespaceServerSideCode.init();
    }

    public static long currentTimeMillis() {
        return timeSource.absTimeMillis();
    }
    
    //public static long currentTimeNanos() {
    //    return timeSource.relTimeNanos();
    //}
    
    public static Timer timer() {
        return timer;
    }
}

package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.util.jvm.Finalization;

public class JVMUtil {
    private static final Finalization    finalization;
    
    static {
        finalization = new Finalization(SystemTimeUtil.timerDrivenTimeSource, true);
    }

    public static Finalization getGlobalFinalization() {
        return finalization;
    }
}

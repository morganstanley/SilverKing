package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.util.jvm.Finalization;

public class JVMUtil {
    public static final Finalization	finalization;
    
    static {
        finalization = new Finalization(SystemTimeUtil.timerDrivenTimeSource, true);
    }
}

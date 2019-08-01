package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.jvm.Finalization;

public class JVMUtil {
    private static final Finalization    finalization;

    public static final String    verboseFinalizationPropertyName = "com.ms.silverking.cloud.dht.common.VerboseGlobalFinalization";

    static {
        boolean verboseFinalization = PropertiesHelper.systemHelper.getBoolean(verboseFinalizationPropertyName, true);
        finalization = new Finalization(SystemTimeUtil.timerDrivenTimeSource, verboseFinalization);
    }

    public static Finalization getGlobalFinalization() {
        return finalization;
    }
}

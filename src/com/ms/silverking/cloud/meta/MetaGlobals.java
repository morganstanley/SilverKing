package com.ms.silverking.cloud.meta;

import java.util.Timer;

import com.ms.silverking.util.SafeTimer;

public class MetaGlobals {
    public static final String timerName = "MetaGlobalTimer";
    public static final Timer  timer;
    
    static {
        timer = new SafeTimer(timerName, true);
    }
}

package com.ms.silverking.util;

import com.ms.silverking.log.Log;

public class DebugUtil {    
    private static final long startTimeMillis;
    private static boolean delayExpired = false;
    
    private static long debugStartMS = 1000 * 60 * 1000;
    
    static {
        startTimeMillis = System.currentTimeMillis();
        new DelayedDebugTester();
    }
    
    public static boolean delayedDebug() {
        return delayExpired;
    }
    
    static class DelayedDebugTester implements Runnable {
        private static long testIntervalMillis = 1000;
        
        DelayedDebugTester() {
            Log.warning("Delayed debugging will trigger in: "+ debugStartMS);
            new Thread(this).start();
        }
        
        @Override
        public void run() {
            while (!delayExpired) {
                try {
                    Thread.sleep(testIntervalMillis);
                    if (System.currentTimeMillis() - startTimeMillis > debugStartMS) {
                        delayExpired = true;
                        Log.warning("Delayed debugging triggered");
                        Log.setLevelAll();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

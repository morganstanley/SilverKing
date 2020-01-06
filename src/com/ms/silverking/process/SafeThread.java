package com.ms.silverking.process;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListSet;

import com.ms.silverking.log.Log;

/**
 * Thread class with pre-configured uncaught exception handler
 */
public class SafeThread extends Thread implements Comparable<SafeThread> {

    private static GridUncaughtExceptionHandler         exceptionHandler;
    private static ConcurrentSkipListSet<SafeThread>    runningThreads;

    static {
        exceptionHandler = new GridUncaughtExceptionHandler();
        runningThreads = new ConcurrentSkipListSet<SafeThread>();
        setDefaultUncaughtExceptionHandler(exceptionHandler);
    }
    
    public SafeThread(Runnable target, String name) {
        this(target, name, null, false);
    }
    
    public SafeThread(Runnable target, String name, boolean daemon) {
        this(target, name, null, daemon);
    }
    
    public SafeThread(Runnable target, String name, UncaughtExceptionHandler customHandler) {
        this(target, name, customHandler, false);
    }
    
    public SafeThread(Runnable target, String name, UncaughtExceptionHandler customHandler, boolean daemon) {
        super(target, name);
        this.setDaemon(daemon);
        if (customHandler != null) {
            this.setUncaughtExceptionHandler(customHandler);
        }
    }
    
    public static ArrayList<SafeThread>    getRunningThreads() {
        ArrayList<SafeThread>    result;
        result = new ArrayList<SafeThread>();
        result.addAll(runningThreads);
        return result;
    }
    
    public void run() {
        try {
            runningThreads.add(this);
            super.run();
        } finally {
            runningThreads.remove(this);
        }
    }

    public int compareTo(SafeThread o) {
        int h1 = this.hashCode(), h2 = o.hashCode();
        if (h1 < h2) return -1;
        if (h1 > h2) return 1;
        return 0;
    }
    
    private static class GridUncaughtExceptionHandler implements UncaughtExceptionHandler {
        
        public GridUncaughtExceptionHandler() {}
        
        public void uncaughtException(Thread t, Throwable e) {
            try {
                Log.logErrorSevere(e, "GridUncaughtException", "defaultHandler");
            } catch (Throwable x) {} // Ensure we get to System.exit
            System.exit(1);
        }

    }    

}

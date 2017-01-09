package com.ms.silverking.thread.lwt;

import java.lang.Thread.UncaughtExceptionHandler;

import com.ms.silverking.log.Log;

public class LWTUncaughtExceptionHandler implements UncaughtExceptionHandler {

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        Log.logErrorWarning(e, "Thread "+ t.getName() +" threw an uncaught exception");
    }
}

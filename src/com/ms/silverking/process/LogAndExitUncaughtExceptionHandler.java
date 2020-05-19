package com.ms.silverking.process;

import java.lang.Thread.UncaughtExceptionHandler;

import com.ms.silverking.log.Log;

public class LogAndExitUncaughtExceptionHandler implements UncaughtExceptionHandler {
  public LogAndExitUncaughtExceptionHandler() {}

  public void uncaughtException(Thread t, Throwable e) {
    try {
      Log.logErrorSevere(e, "UncaughtException", "defaultHandler");
    } catch (Throwable x) {} // Ensure we get to System.exit
    System.exit(1);
  }
}
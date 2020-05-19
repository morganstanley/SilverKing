package com.ms.silverking.process;

import java.lang.Thread.UncaughtExceptionHandler;

import com.ms.silverking.log.Log;

public class LogOnlyUncaughtExceptionHandler implements UncaughtExceptionHandler {
  public LogOnlyUncaughtExceptionHandler() {}

  public void uncaughtException(Thread t, Throwable e) {
    try {
      Log.logErrorSevere(e, "UncaughtException", "defaultHandler");
    } catch (Throwable x) {}
  }
}
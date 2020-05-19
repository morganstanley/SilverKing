package com.ms.silverking.thread.lwt;

import java.lang.Thread.UncaughtExceptionHandler;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

public class LWTUncaughtExceptionHandler implements UncaughtExceptionHandler {
  private static final int uncaughtExceptionExitDelayMillis = 4 * 1000;

  /**
   * Display information about the uncaught exception and then force the JVM to exit.
   * We prefer fail-stop behavior to unexpected behavior.
   */
  @Override
  public void uncaughtException(Thread t, Throwable e) {
    Log.logErrorWarning(e, "Thread " + t.getName() + " threw an uncaught exception");
    ThreadUtil.sleep(uncaughtExceptionExitDelayMillis);
    System.exit(1);
  }
}

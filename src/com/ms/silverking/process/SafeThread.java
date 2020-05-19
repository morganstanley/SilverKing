package com.ms.silverking.process;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * Thread class with pre-configured uncaught exception handler
 */
public class SafeThread extends Thread implements Comparable<SafeThread> {
  private static ConcurrentSkipListSet<SafeThread> runningThreads;

  private static final String defaultUncaughtExceptionHandlerProperty = SafeThread.class.getName() +
      ".DefaultUncaughtExceptionHandler";

  static {
    String handlerName;
    UncaughtExceptionHandler handler;

    /*
     * We should never have uncaught exceptions. The default exception handler
     * logs the exception and exits. The purpose is to fail fast and visibly so
     * that any uncaught exceptions are noticed and fixed, rather than
     * silently ignored.
     */

    handlerName = PropertiesHelper.systemHelper.getString(defaultUncaughtExceptionHandlerProperty,
        UndefinedAction.ZeroOnUndefined);
    Log.infof("UncaughtExceptionHandler %s", handlerName == null ? "<none>" : handlerName);
    if (handlerName != null) {
      try {
        handler = (UncaughtExceptionHandler) Class.forName(handlerName).newInstance();
        setDefaultUncaughtExceptionHandler(handler);
      } catch (Exception e) {
        Log.logErrorSevere(e, "Unable to create UncaughtExceptionHandler: " + handlerName, SafeThread.class.getName(),
            "static {}");
        System.exit(-1);
      }
    }
    runningThreads = new ConcurrentSkipListSet<SafeThread>();
  }

  public static void setDefaultUncaughtExceptionHandler(UncaughtExceptionHandler handler) {
    Log.infof("setDefaultUncaughtExceptionHandler: %s", handler);
    Thread.setDefaultUncaughtExceptionHandler(handler);
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

  public static List<SafeThread> getRunningThreads() {
    List<SafeThread> result;

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
    int h1 = this.hashCode();
    int h2 = o.hashCode();
    if (h1 < h2)
      return -1;
    if (h1 > h2)
      return 1;
    return 0;
  }
}

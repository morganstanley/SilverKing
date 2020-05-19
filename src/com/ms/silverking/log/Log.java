package com.ms.silverking.log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import com.ms.silverking.alert.Alert;
import com.ms.silverking.alert.AlertReceiver;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * Log constants and utilities
 */
public final class Log {
  // FUTURE - This implementation is derived from a legacy implementation.
  // Update this in the future.

  private static LogDest logDest;
  private static Level __level;
  private static int _level; // store as int for performance

  private static AlertReceiver alertReceiver;
  private static int alertLevel;
  private static String alertContext;
  private static String alertKey;
  private static String alertData;

  static final BlockingQueue<LogEntry> logQueue;
  static final AtomicBoolean asyncLogRunning;

  private static final int maxAsyncQueueSize = 10000;

  private static final String javaLogDestValue = "Java";
  private static final String log4jLogDestValue = "log4j";
  private static final LogDest log4jLogDest = new Log4jLogDest();
  private static final LogDest javaLogDest = new JavaLogDest();
  private static final String defaultLogDestProperty = javaLogDestValue;
  private static final String propertyBase = "com.ms.silverking.";
  private static final String logDestProperty = propertyBase + "LogDest";
  private static final String logLevelProperty = propertyBase + "Log";

  private static final String logAlertLevelEnvVar = "SK_LOG_ALERT_LEVEL";
  private static final String logAlertReceiverEnvVar = "SK_LOG_ALERT_RECEIVER";
  private static final String logAlertContextEnvVar = "SK_LOG_ALERT_CONTEXT";
  private static final String logAlertKeyEnvVar = "SK_LOG_ALERT_KEY";
  private static final String logAlertDataEnvVar = "SK_LOG_ALERT_DATA";
  private static final String defaultLogAlertContext = "SilverKing";

  static {
    String val;
    String logLevel;

    val = PropertiesHelper.systemHelper.getString(logDestProperty, defaultLogDestProperty);
    if (val.equalsIgnoreCase(javaLogDestValue)) {
      logDest = javaLogDest;
    } else if (val.equalsIgnoreCase(log4jLogDestValue)) {
      logDest = log4jLogDest;
    } else {
      logDest = javaLogDest;
    }

    logLevel = System.getProperty(logLevelProperty);
    if (logLevel == null) {
      logLevel = "warning";
    }
    if (logLevel.equalsIgnoreCase("all")) {
      setLevelAll();
    } else if (logLevel.equalsIgnoreCase("log")) {
      setLevelLog();
    } else if (logLevel.equalsIgnoreCase("info")) {
      setLevel(Level.INFO);
    } else if (logLevel.equalsIgnoreCase("off")) {
      setLevelOff();
    } else if (logLevel.equalsIgnoreCase("warning")) {
      setLevel(Level.WARNING);
    } else {
      Log.warning("Unknown logging level: " + logLevel);
    }
    logQueue = new LinkedBlockingQueue<LogEntry>(maxAsyncQueueSize);

    val = PropertiesHelper.envHelper.getString(logAlertReceiverEnvVar, UndefinedAction.ZeroOnUndefined);
    if (val != null) {
      try {
        alertReceiver = (AlertReceiver) Class.forName(val).newInstance();
        alertLevel = PropertiesHelper.envHelper.getInt(logAlertLevelEnvVar, Level.SEVERE.intValue());
        alertContext = PropertiesHelper.envHelper.getString(logAlertContextEnvVar, defaultLogAlertContext);
        alertKey = PropertiesHelper.envHelper.getString(logAlertKeyEnvVar, UndefinedAction.ZeroOnUndefined);
        alertData = PropertiesHelper.envHelper.getString(logAlertDataEnvVar, UndefinedAction.ZeroOnUndefined);
        Log.warningf("Log AlertReceiver %s level %d context %s key %s data %s", alertReceiver, alertLevel, alertContext,
            alertKey, alertData);
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        Log.logErrorWarning(e, "Unable to instantiate AlertReceiver");
      }
    } else {
      Log.info("No log AlertReceiver");
    }

    log(Level.FINE, "Logging initialized." + logDest.getClass().getName());

    asyncLogRunning = new AtomicBoolean();
  }

  public static void setLogDest(LogDest dest) {
    logDest = dest;
  }

  public static void initAsyncLogging() {
    boolean alreadyRunning;

    alreadyRunning = asyncLogRunning.getAndSet(true);
    if (!alreadyRunning) {
      new AsyncLogger(logQueue);
    }
  }

  public static boolean levelMet(Level level) {
    return _level <= level.intValue();
  }

  public static Level getLevel() {
    return __level;
  }

  public static void setLevel(String logLevel) {
    setLevel(Level.parse(logLevel));
  }

  public static void setLevel(Level level) {
    synchronized (Log.class) {
      _level = level.intValue();
      __level = level;
      logDest.setLevel(level);
    }
  }

  public static void setLevelAll() {
    setLevel(Level.ALL);
  }

  public static void setLevelOff() {
    setLevel(Level.OFF);
  }

  public static void setLevelLog() {
    setLevel(Level.CONFIG);
  }

  public static void setPrintStreams(String fileName) throws IOException {
    setPrintStreams(new File(fileName));
  }

  public static void setPrintStreams(File file) throws IOException {
    setPrintStreams(new FileOutputStream(file));
  }

  public static void setPrintStreams(OutputStream out) {
    logDest.setPrintStreams(out);
  }

  public static void countdownWarning(String m, int countdownSeconds) {
    countdown(Level.WARNING, m, countdownSeconds);
  }

  public static void countdown(Level level, String m, int countdownSeconds) {
    for (int t = countdownSeconds; t > 0; t--) {
      Log.log(level, "Countdown: " + m + ". " + t + " seconds remaining.");
      ThreadUtil.sleepSeconds(1);
    }
    Log.log(level, "Countdown: " + m + ". Countdown complete.");
  }

  public static void log(Level level, String m) {
    if (alertReceiver != null && level.intValue() >= alertLevel) {
      alertReceiver.sendAlert(new Alert(alertContext, level.intValue(), alertKey != null ? alertKey : m, m, alertData));
    }
    logDest.log(level, m);
  }

  public static void warning(String m, Object o) {
    if (levelMet(Level.WARNING)) {
      warning(m + o);
    }
  }

  public static void warning(Object o) {
    if (o == null) {
      warning("null");
    } else {
      warning(o.toString());
    }
  }

  public static void warning(String m) {
    log(Level.WARNING, m);
  }

  public static void warningAsyncf(String f, Object... args) {
    if (levelMet(Level.WARNING)) {
      warningAsyncInternal(new LogEntry(Level.WARNING, String.format(f, args)));
    }
  }

  public static void warningf(String f, Object... args) {
    if (levelMet(Level.WARNING)) {
      warning(String.format(f, args));
    }
  }

  public static void warningAsync(String m, Object o) {
    if (levelMet(Level.WARNING)) {
      warningAsyncInternal(new LogEntry(Level.WARNING, o, m));
    }
  }

  public static void warningAsync(Object o) {
    if (o == null) {
      warningAsync("null");
    } else {
      warningAsync(o.toString());
    }
  }

  public static void warningAsync(String m) {
    if (levelMet(Level.WARNING)) {
      warningAsyncInternal(new LogEntry(Level.WARNING, m));
    }
  }

  private static void warningAsyncInternal(LogEntry le) {
    try {
      logQueue.put(le);
    } catch (InterruptedException ie) {
    }
  }

  public static void severe(String m, String className, String methodName) {
    log(Level.SEVERE, className + "." + methodName + ": " + m);
  }

  public static void severe(String m, Object o) {
    if (levelMet(Level.SEVERE)) {
      severe(m + o);
    }
  }

  public static void severe(Object o) {
    if (o == null) {
      severe("null");
    } else {
      severe(o.toString());
    }
  }

  public static void severe(String m) {
    log(Level.SEVERE, m);
  }

  public static void severeAsyncf(String f, Object... args) {
    if (levelMet(Level.SEVERE)) {
      severeAsync(String.format(f, args));
    }
  }

  public static void severef(String f, Object... args) {
    if (levelMet(Level.SEVERE)) {
      severe(String.format(f, args));
    }
  }

  public static void severeAsync(String m, Object o) {
    if (levelMet(Level.SEVERE)) {
      try {
        logQueue.put(new LogEntry(Level.SEVERE, o, m));
      } catch (InterruptedException ie) {
      }
    }
  }

  public static void severeAsync(Object o) {
    if (o == null) {
      severeAsync("null");
    } else {
      severeAsync(o.toString());
    }
  }

  public static void severeAsync(String m) {
    if (levelMet(Level.SEVERE)) {
      try {
        logQueue.put(new LogEntry(Level.SEVERE, m));
      } catch (InterruptedException ie) {
      }
    }
  }

  public static void infoAsync(String m, Object o) {
    if (levelMet(Level.INFO)) {
      infoAsyncInternal(new LogEntry(Level.INFO, o, m));
    }
  }

  public static void infoAsync(String m) {
    if (levelMet(Level.INFO)) {
      infoAsyncInternal(new LogEntry(Level.INFO, m));
    }
  }

  public static void infoAsyncf(String f, Object... args) {
    if (levelMet(Level.INFO)) {
      infoAsyncInternal(new LogEntry(Level.INFO, String.format(f, args)));
    }
  }

  private static void infoAsyncInternal(LogEntry le) {
    try {
      logQueue.put(le);
    } catch (InterruptedException ie) {
    }
  }

  public static void info(String m, Object o) {
    if (levelMet(Level.INFO)) {
      info(m + o);
    }
  }

  public static void info(Object o) {
    if (Log.levelMet(Level.INFO)) {
      if (o != null) {
        info(o.toString());
      } else {
        info("null");
      }
    }
  }

  public static void info(String m) {
    if (Log.levelMet(Level.INFO)) {
      log(Level.INFO, m);
    }
  }

  public static void infof(String f, Object... args) {
    if (levelMet(Level.INFO)) {
      info(String.format(f, args));
    }
  }

  public static void log(Object o) {
    if (Log.levelMet(Level.INFO)) {
      if (o != null) {
        log(o.toString());
      } else {
        log("null");
      }
    }
  }

  public static void log(String m, Object o) {
    if (levelMet(Level.INFO)) {
      log(m + o);
    }
  }

  public static void log(String m) {
    log(Level.INFO, m);
  }

  private static void logError(Throwable e, String msg, Level l) {
    logDest.logError(l, msg, e);
  }

  public static void logErrorWarning(Throwable e) {
    logError(e, e.getMessage(), Level.WARNING);
  }

  public static void logErrorSevere(Throwable e, String className, String methodName) {
    logErrorSevere(e, null, className, methodName);
  }

  public static void logErrorWarning(Throwable e, String msg) {
    logError(e, msg, Level.WARNING);
  }

  public static void logErrorSevere(Throwable e, String msg, String className, String methodName) {
    logError(e, msg, Level.SEVERE);
  }

  public static void fineAsync(String m, Object o) {
    if (levelMet(Level.FINE)) {
      fineAsyncInternal(new LogEntry(Level.FINE, o, m));
    }
  }

  public static void fineAsync(Object o) {
    if (levelMet(Level.FINE)) {
      fineAsyncInternal(new LogEntry(Level.FINE, o.toString()));
    }
  }

  public static void fineAsync(String m) {
    if (levelMet(Level.FINE)) {
      fineAsyncInternal(new LogEntry(Level.FINE, m));
    }
  }

  private static void fineAsyncInternal(LogEntry le) {
    try {
      logQueue.put(le);
    } catch (InterruptedException ie) {
    }
  }

  public static void fine(String m, Object o) {
    if (levelMet(Level.FINE)) {
      fine(m + o);
    }
  }

  public static void fine(Object o) {
    if (levelMet(Level.FINE)) {
      if (o == null) {
        fine("null");
      } else {
        fine(o.toString());
      }
    }
  }

  public static void finef(String f, Object... args) {
    if (levelMet(Level.FINE)) {
      fine(String.format(f, args));
    }
  }

  public static void fineAsyncf(String f, Object... args) {
    if (levelMet(Level.FINE)) {
      fineAsyncInternal(new LogEntry(Level.FINE, String.format(f, args)));
    }
  }

  public static void fine(String m) {
    if (Log.levelMet(Level.FINE)) {
      log(Level.FINE, m);
    }
  }

  public static void dumpStack() {
    try {
      throw new RuntimeException("stacktrace");
    } catch (RuntimeException re) {
      logErrorWarning(re);
    }
  }
}

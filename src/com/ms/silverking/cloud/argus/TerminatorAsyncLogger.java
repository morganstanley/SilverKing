package com.ms.silverking.cloud.argus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

import com.ms.silverking.log.HighTemporalResolutionFormatter;
import com.ms.silverking.log.Log;
import com.ms.silverking.process.SafeThread;
import com.ms.silverking.time.TimeUtils;

/**
 * TerminatorAsyncLogger starts a separate thread and
 * asynchronously writes log messages that comes from Terminator.
 * Each message contains timestamp, a reason for termination
 * and a PID.
 */
public class TerminatorAsyncLogger implements Runnable {
  private static final String argusLoggerName = "com.ms.silverking.cloud.argus.TerminatorAsyncLogger";
  private final Thread asyncLogger;
  private final String logFileName;
  private AtomicBoolean running;
  private BlockingQueue<String> logMessages;
  private Logger log;
  private StreamHandler handler;

  public TerminatorAsyncLogger(String loggerFileName) {
    running = new AtomicBoolean(true);
    logMessages = new LinkedBlockingQueue<String>();
    logFileName = loggerFileName;
    asyncLogger = new SafeThread(this, argusLoggerName, true);
    asyncLogger.start();
  }

  private Logger getLogger() throws FileNotFoundException {
    synchronized (this) { //not really needed yet, but TerminatorAsyncLogger is exposed to more than one thread
      if (log == null) {
        PrintStream ps = new PrintStream(new File(logFileName));
        log = Logger.getLogger(argusLoggerName);
        Formatter formatter = new HighTemporalResolutionFormatter();
        handler = new StreamHandler(ps, formatter);
        log.addHandler(handler);
        log.setLevel(Level.ALL);
      }
      return log;
    }
  }

  void add(String msg) {

    logMessages.add(TimeUtils.getCurrentTimeString() + " : " + msg);
  }

  public void stopRunning() {
    running.set(false);

    /* asyncLogger is very likely to be blocked in logMessages.take()
     * when no log messages in the queue, we can
     * *either*  send an empty message *or* call interrupt()
     * to re-read 'running' var and terminate messageHandler thread.
     */
    add("");
    //asyncLogger.interrupt();
  }

  public void run() {
    Log.fine("TerminatorAsyncLogger running");
    while (running.get()) {
      String terminatedProcMessage;

      try {
        terminatedProcMessage = logMessages.take();
        try {
          getLogger().log(Level.WARNING, terminatedProcMessage);
          handler.flush();
        } catch (FileNotFoundException e) {
          Log.logErrorWarning(e);
        }

      } catch (InterruptedException ie) {
        continue;
      }
    }
    Log.fine("TerminatorAsyncLogger stopping");
  }

  /**
   * A method for testing TerminatorAsyncLogger
   */
  public static void test() {
    try {
      Log.setLevelAll();
      Log.setPrintStreams(System.out);  //main log

      //create a temporary file name for async logger
      File tmpFile = File.createTempFile("terminatorAsyncLogger.test", ".log");
      tmpFile.delete();  //remove the file, it should be created during lazy init.
      String fileName = tmpFile.getAbsolutePath();
      Log.fine("terminatorAsyncLogger log file: " + fileName);

      //create an object of this class
      TerminatorAsyncLogger lmh = new TerminatorAsyncLogger(fileName);

      //add messages
      for (int i = 0; i < 20; i++) {
        lmh.add("LogMessage Test" + i);
      }
      Thread.sleep(100);
      lmh.stopRunning();

    } catch (Exception ex) {
      System.err.println(ex);
    }
  }

  /**
   * @param args cmd-line args
   */
  public static void main(String[] args) {
    //run basic test, no arguments are used
    test();
  }

}

package com.ms.silverking.util.jvm;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.log.Log;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class Finalization {
  private final AbsMillisTimeSource absMillisTimeSource;
  private final Lock lock;
  private final boolean verboseFinalization;
  private long lastFinalizationMillis;

  public Finalization(AbsMillisTimeSource absMillisTimeSource, boolean verboseFinalization) {
    this.absMillisTimeSource = absMillisTimeSource;
    this.verboseFinalization = verboseFinalization;
    lastFinalizationMillis = absMillisTimeSource.absTimeMillis();
    lock = new ReentrantLock();
  }

  public boolean forceFinalization(long minFinalizationIntervalMillis) {
    if (minFinalizationIntervalMillis < 0) {
      throw new RuntimeException("minFinalizationIntervalMillis may not be < 0");
    } else {
      long curTimeMillis;
      boolean finalized;

      curTimeMillis = absMillisTimeSource.absTimeMillis(); // ignore lock acquisition time
      lock.lock();
      try {
        // minFinalizationIntervalMillis == 0 => ignore lastFinalizationMillis and force a finalization
        if ((curTimeMillis - lastFinalizationMillis > minFinalizationIntervalMillis) || (minFinalizationIntervalMillis == 0)) {
          Stopwatch sw;

          lastFinalizationMillis = curTimeMillis;
          sw = new SimpleStopwatch();
          System.gc();
          System.runFinalization();
          sw.stop();
          finalized = true;
          if (verboseFinalization) {
            Log.warningf("Finalization completed in %f seconds\n", sw.getElapsedSeconds());
          }
        } else {
          finalized = false;
        }
      } finally {
        lock.unlock();
      }
      return finalized;
    }
  }
}

package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.time.SystemTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;
import com.ms.silverking.util.SafeTimer;

public class SystemTimeUtil {
  private static final int timerDrivenTimeSourceResolutionMS = 5;
  private static final String timeSourceTimerName = "TimeSourceTimer";

  /**
   * Time source that returns time from Java system time calls.
   * Absolute nanos times are based on elapsed nanoseconds since midnight January 1, 2000.
   */
  public static final SystemTimeSource skSystemTimeSource = SystemTimeSource.createWithMillisOrigin(
      DHTConstants.nanoOriginTimeInMillis);
  /**
   * Time driven time source for obtaining granular time with extremely low-overhead
   */
  public static final TimerDrivenTimeSource timerDrivenTimeSource = new TimerDrivenTimeSource(
      new SafeTimer(timeSourceTimerName, true), timerDrivenTimeSourceResolutionMS);

  public static final long systemTimeNanosToEpochMillis(long nanos) {
    //absTimeNanos = (absTimeMillis - nanoOriginTimeMillis) * nanosPerMilli;
    return (nanos / 1000000) + DHTConstants.nanoOriginTimeInMillis;
  }
}

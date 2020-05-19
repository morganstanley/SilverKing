package com.ms.silverking.time;

public interface AbsNanosTimeSource {
  /**
   * Return the origin time in nanoseconds
   *
   * @return the origin time in nanoseconds
   */
  public long getNanosOriginTime();

  /**
   * @return the difference, measured in nanoseconds, between the current time and the origin time.
   */
  public long absTimeNanos();

  /**
   * @param absDeadlineNanos TODO
   * @return the difference between absDeadlineNanos and absTimeNanos()
   */
  public long relNanosRemaining(long absDeadlineNanos);

}

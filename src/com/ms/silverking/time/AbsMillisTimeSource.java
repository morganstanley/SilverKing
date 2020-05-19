package com.ms.silverking.time;

/**
 * Provides absolute time in milliseconds.
 */
public interface AbsMillisTimeSource {
  /**
   * @return the difference, measured in milliseconds, between the current time and midnight, January 1, 1970 UTC.
   */
  public long absTimeMillis();

  /**
   * @param absDeadlineMillis TODO
   * @return the difference between absDeadlineMillis and absTimeMillis()
   */
  public int relMillisRemaining(long absDeadlineMillis);
}

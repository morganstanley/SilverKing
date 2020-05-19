package com.ms.silverking.time;

import java.math.BigDecimal;
import java.util.concurrent.locks.Condition;

public interface Timer extends Stopwatch {
  /**
   * True iff the timer has expired
   *
   * @return true iff the timer has expired
   */
  public boolean hasExpired();

  /**
   * Wait uninterruptibly for expiration
   */
  public void waitForExpiration();

  /* Remaining time methods */

  /**
   * Get the remaining time in nanoseconds.
   *
   * @return the remaining time in nanoseconds
   */
  public long getRemainingNanos();

  /**
   * Get the remaining time in milliseconds (as a long.)
   *
   * @return the remaining time in milliseconds (as a long)
   */
  public long getRemainingMillisLong();

  /**
   * Get the remaining time in milliseconds.
   *
   * @return the remaining time in milliseconds
   */
  public int getRemainingMillis();

  /**
   * Get the remaining time in seconds.
   *
   * @return the remaining time in seconds
   */
  public double getRemainingSeconds();

  /**
   * Get the remaining time in seconds (as a BigDecimal.)
   *
   * @return the remaining time in seconds (as a BigDecimal)
   */
  public BigDecimal getRemainingSecondsBD();

  /* TimeLimit time methods */

  /**
   * Get the TimeLimit time in nanoseconds.
   *
   * @return the TimeLimit time in nanoseconds
   */
  public long getTimeLimitNanos();

  /**
   * Get the TimeLimit time in milliseconds (as a long.)
   *
   * @return the TimeLimit time in milliseconds (as a long)
   */
  public long getTimeLimitMillisLong();

  /**
   * Get the TimeLimit time in milliseconds.
   *
   * @return the TimeLimit time in milliseconds
   */
  public int getTimeLimitMillis();

  /**
   * Get the TimeLimit time in seconds.
   *
   * @return the TimeLimit time in seconds
   */
  public double getTimeLimitSeconds();

  /**
   * Get the TimeLimit time in seconds (as a BigDecimal.)
   *
   * @return the TimeLimit time in seconds (as a BigDecimal)
   */
  public BigDecimal getTimeLimitSecondsBD();

  /**
   * Wait on the provided Condition for the remaining time
   *
   * @param cv TODO
   * @return {@code false} if the waiting time detectably elapsed
   * before return from the method, else {@code true}
   * @throws InterruptedException TODO
   */
  public boolean await(Condition cv) throws InterruptedException;
}

package com.ms.silverking.net.async;

import com.ms.silverking.net.AddrAndPort;

/**
 * Controls retry and timeout behavior of new connections
 */
public interface NewConnectionTimeoutController {
  /**
   * Return the maximum number of times that this connection establishment should be attempted.
   *
   * @param addrAndPort the target of the connection being established
   * @return the maximum number of times that this connection should be attempted
   */
  public int getMaxAttempts(AddrAndPort addrAndPort);

  /**
   * Return the relative timeout in milliseconds for the given attempt.
   *
   * @param addrAndPort  the target of the connection being established
   * @param attemptIndex a zero-based attempt index. Ranges from 0 to the maximum number of attempts - 1.
   * @return the relative timeout in milliseconds for the given attempt
   */
  public long getRelativeTimeoutMillisForAttempt(AddrAndPort addrAndPort, int attemptIndex);

  /**
   * Return the maximum relative timeout for the given connection establishment.
   * Once this timeout is triggered, no further attempts will be made irrespective
   * of the individual attempt timeout or the maximum number of attempts.
   *
   * @param addrAndPort the target of the connection being established
   * @return the maximum relative timeout for the given operation
   */
  public long getMaxRelativeTimeoutMillis(AddrAndPort addrAndPort);
}

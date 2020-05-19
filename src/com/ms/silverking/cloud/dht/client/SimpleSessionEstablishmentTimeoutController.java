package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.text.ObjectDefParser2;

public class SimpleSessionEstablishmentTimeoutController implements SessionEstablishmentTimeoutController {
  private final int maxAttempts;
  private final int attemptRelativeTimeoutMillis;
  private final int maxRelativeTimeoutMillis;

  private static final int defaultMaxAttempts = 4;
  private static final int defaultAttemptRelativeTimeoutMillis = 2 * 60 * 1000;
  private static final int defaultMaxRelativeTimeoutMillis = 8 * 60 * 1000;

  private static final SimpleSessionEstablishmentTimeoutController template =
      new SimpleSessionEstablishmentTimeoutController(
      defaultMaxAttempts, defaultAttemptRelativeTimeoutMillis, defaultMaxRelativeTimeoutMillis);

  static {
    ObjectDefParser2.addParser(template);
  }

  public SimpleSessionEstablishmentTimeoutController(int maxAttempts, int attemptRelativeTimeoutMillis,
      int maxRelativeTimeoutMillis) {
    this.maxAttempts = maxAttempts;
    this.attemptRelativeTimeoutMillis = attemptRelativeTimeoutMillis;
    this.maxRelativeTimeoutMillis = maxRelativeTimeoutMillis;
  }

  /**
   * Create a SimpleConnectionEstablishmentTimeoutController like this instance, but with a new maxAttempts.
   *
   * @param maxAttempts TODO
   * @return a SimpleConnectionEstablishmentTimeoutController like this instance, but with a new maxAttempts
   */
  public SimpleSessionEstablishmentTimeoutController maxAttempts(int maxAttempts) {
    return new SimpleSessionEstablishmentTimeoutController(maxAttempts, attemptRelativeTimeoutMillis,
        maxRelativeTimeoutMillis);
  }

  /**
   * Create a SimpleConnectionEstablishmentTimeoutController like this instance, but with a new
   * attemptRelativeTimeoutMillis.
   *
   * @param attemptRelativeTimeoutMillis TODO
   * @return a SimpleConnectionEstablishmentTimeoutController like this instance, but with a new
   * attemptRelativeTimeoutMillis
   */
  public SimpleSessionEstablishmentTimeoutController attemptRelativeTimeoutMillis(int attemptRelativeTimeoutMillis) {
    return new SimpleSessionEstablishmentTimeoutController(maxAttempts, attemptRelativeTimeoutMillis,
        maxRelativeTimeoutMillis);
  }

  /**
   * Create a SimpleTimeoutController like this instance, but with a new maxRelativeTimeoutMillis.
   *
   * @param maxRelativeTimeoutMillis TODO
   * @return a SimpleTimeoutController like this instance, but with a new maxRelativeTimeoutMillis
   */
  public SimpleSessionEstablishmentTimeoutController maxRelativeTimeoutMillis(int maxRelativeTimeoutMillis) {
    return new SimpleSessionEstablishmentTimeoutController(maxAttempts, attemptRelativeTimeoutMillis,
        maxRelativeTimeoutMillis);
  }

  @Override
  public int getMaxAttempts(SessionOptions options) {
    return maxAttempts;
  }

  @Override
  public int getRelativeTimeoutMillisForAttempt(SessionOptions options, int attemptIndex) {
    return attemptRelativeTimeoutMillis;
  }

  @Override
  public int getMaxRelativeTimeoutMillis(SessionOptions options) {
    return maxRelativeTimeoutMillis;
  }

  @Override
  public int hashCode() {
    return maxAttempts ^ attemptRelativeTimeoutMillis ^ maxRelativeTimeoutMillis;
  }

  @Override
  public boolean equals(Object other) {
    SimpleSessionEstablishmentTimeoutController o;

    o = (SimpleSessionEstablishmentTimeoutController) other;
    return maxAttempts == o.maxAttempts && attemptRelativeTimeoutMillis == o.attemptRelativeTimeoutMillis && maxRelativeTimeoutMillis == o.maxRelativeTimeoutMillis;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  /**
   * Parse a definition
   *
   * @param def object definition
   * @return a parsed instance
   */
  public static SimpleSessionEstablishmentTimeoutController parse(String def) {
    return ObjectDefParser2.parse(SimpleSessionEstablishmentTimeoutController.class, def);
  }
}

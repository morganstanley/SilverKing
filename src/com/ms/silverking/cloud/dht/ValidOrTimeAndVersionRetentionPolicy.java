package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

public class ValidOrTimeAndVersionRetentionPolicy implements ValueRetentionPolicy {
  private final Mode mode;
  private final int minVersions;
  private final long timeSpanSeconds;

  public enum Mode {wallClock, mostRecentValue}

  static final ValidOrTimeAndVersionRetentionPolicy template = new ValidOrTimeAndVersionRetentionPolicy(Mode.wallClock,
      1, 86400);

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public ValidOrTimeAndVersionRetentionPolicy(Mode mode, int minVersions, long timeSpanSeconds) {
    this.mode = mode;
    this.minVersions = minVersions;
    this.timeSpanSeconds = timeSpanSeconds;
  }

  public Mode getMode() {
    return mode;
  }

  public int getMinVersions() {
    return minVersions;
  }

  public long getTimeSpanSeconds() {
    return timeSpanSeconds;
  }

  public long getTimeSpanNanos() {
    return timeSpanSeconds * 1000000000;
  }

  public long getTimeSpanMillis() {
    return timeSpanSeconds * 1000;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  @Override
  public int hashCode() {
    return mode.hashCode() ^ Integer.hashCode(minVersions) ^ Long.hashCode(timeSpanSeconds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    ValidOrTimeAndVersionRetentionPolicy other;

    other = (ValidOrTimeAndVersionRetentionPolicy) o;
    return mode == other.mode && minVersions == other.minVersions && timeSpanSeconds == other.timeSpanSeconds;
  }

  /**
   * Parse a definition
   *
   * @param def object definition
   * @return a parsed instance
   */
  public static ValidOrTimeAndVersionRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(ValidOrTimeAndVersionRetentionPolicy.class, def);
  }
}

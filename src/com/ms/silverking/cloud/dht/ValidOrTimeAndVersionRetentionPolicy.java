package com.ms.silverking.cloud.dht;

import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.text.ObjectDefParser2;

public class ValidOrTimeAndVersionRetentionPolicy implements ValueRetentionPolicy<TimeAndVersionRetentionState> {
  private final Mode mode;
  private final int minVersions;
  private final long timeSpanSeconds;

  public enum Mode {wallClock, mostRecentValue}

  ;

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

  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      TimeAndVersionRetentionState timeRetentionState, long curTimeNanos, long storedLength) {
    int totalVersions;
    long mostRecentCreationTimeNanos;
    long spanEndTimeNanos;
    Pair<Integer, Long> vData;
    long deltaNanos;

    vData = timeRetentionState.processValue(key, creationTimeNanos);
    totalVersions = vData.getV1();
    mostRecentCreationTimeNanos = vData.getV2();
    if (mode == Mode.wallClock) {
      spanEndTimeNanos = SystemTimeUtil.skSystemTimeSource.absTimeNanos();
    } else {
      spanEndTimeNanos = mostRecentCreationTimeNanos;
    }
    deltaNanos = spanEndTimeNanos - creationTimeNanos;
    //System.out.printf("%s %d %s %d %d\t%d\t%d\t%s\n", key, version, invalidated, creationTimeNanos,
    // spanEndTimeNanos, totalVersions, deltaNanos, totalVersions > 1 ? "_G_" : "_NG_");
    return (totalVersions <= 1 && !invalidated) // retain most recent value, if it's valid; for all other values, use
        // time and version retention policy
        || totalVersions <= minVersions || deltaNanos <= TimeUnit.NANOSECONDS.convert(timeSpanSeconds,
        TimeUnit.SECONDS);
  }

  @Override
  public TimeAndVersionRetentionState createInitialState(PutTrigger putTrigger, RetrieveTrigger retrieveTrigger) {
    return new TimeAndVersionRetentionState();
  }

  @Override
  public boolean considersStoredLength() {
    return false;
  }
}

package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

public class NanosVersionRetentionPolicy implements ValueRetentionPolicy {
  private final long invalidatedRetentionIntervalSeconds;
  private final long maxRetentionIntervalSeconds;

  public static final long NO_MAX_RETENTION_INTERVAL = 0;

  static final NanosVersionRetentionPolicy template = new NanosVersionRetentionPolicy(0, 0);

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public NanosVersionRetentionPolicy(long invalidatedRetentionIntervalSeconds, long maxRetentionIntervalSeconds) {
    this.invalidatedRetentionIntervalSeconds = invalidatedRetentionIntervalSeconds;
    this.maxRetentionIntervalSeconds = maxRetentionIntervalSeconds;
  }

  @OmitGeneration
  public NanosVersionRetentionPolicy(long invalidatedRetentionIntervalSeconds) {
    this(invalidatedRetentionIntervalSeconds, NO_MAX_RETENTION_INTERVAL);
  }

  public long getInvalidatedRetentionIntervalSeconds() {
    return invalidatedRetentionIntervalSeconds;
  }

  public long getMaxRetentionIntervalSeconds() {
    return maxRetentionIntervalSeconds;
    }

  @Override
  public int hashCode() {
    return Long.hashCode(invalidatedRetentionIntervalSeconds) ^ Long.hashCode(maxRetentionIntervalSeconds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    NanosVersionRetentionPolicy other;

    other = (NanosVersionRetentionPolicy) o;
    return invalidatedRetentionIntervalSeconds == other.invalidatedRetentionIntervalSeconds && maxRetentionIntervalSeconds == other.maxRetentionIntervalSeconds;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static NanosVersionRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(NanosVersionRetentionPolicy.class, def);
  }
}

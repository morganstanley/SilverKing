package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.ObjectDefParser2;

public class InvalidatedRetentionPolicy implements ValueRetentionPolicy {
  private final long invalidatedRetentionIntervalSeconds;

  static final InvalidatedRetentionPolicy template = new InvalidatedRetentionPolicy();

  static {
    ObjectDefParser2.addParser(template);
  }

  private InvalidatedRetentionPolicy() {
    this(0);
  }

  @OmitGeneration
  public InvalidatedRetentionPolicy(long invalidatedRetentionIntervalSeconds) {
    this.invalidatedRetentionIntervalSeconds = invalidatedRetentionIntervalSeconds;
  }

  public long getInvalidatedRetentionIntervalSeconds() {
    return invalidatedRetentionIntervalSeconds;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(invalidatedRetentionIntervalSeconds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    InvalidatedRetentionPolicy other;

    other = (InvalidatedRetentionPolicy) o;
    return invalidatedRetentionIntervalSeconds == other.invalidatedRetentionIntervalSeconds;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static InvalidatedRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(InvalidatedRetentionPolicy.class, def);
  }
}

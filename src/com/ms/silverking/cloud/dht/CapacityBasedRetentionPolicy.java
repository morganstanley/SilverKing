package com.ms.silverking.cloud.dht;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

abstract class CapacityBasedRetentionPolicy implements ValueRetentionPolicy {
  static {
    ObjectDefParser2.addParserWithExclusions(CapacityBasedRetentionPolicy.class, null,
        FieldsRequirement.ALLOW_INCOMPLETE, null);
  }

  protected final long capacityBytes;

  public CapacityBasedRetentionPolicy(long capacityBytes) {
    this.capacityBytes = capacityBytes;
  }

  public long getCapacityBytes() {
    return capacityBytes;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(capacityBytes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    CapacityBasedRetentionPolicy other;

    other = (CapacityBasedRetentionPolicy) o;
    return capacityBytes == other.capacityBytes;
  }
}

package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Base class for use by retention policies that consider a (per server) capacity limit.
 */
public abstract class CapacityBasedRetentionPolicy<T extends ValueRetentionState> implements ValueRetentionPolicy<T> {
  protected final long capacityBytes;

  static {
    ObjectDefParser2.addParserWithExclusions(CapacityBasedRetentionPolicy.class, null,
        FieldsRequirement.ALLOW_INCOMPLETE, null);
  }

  @OmitGeneration
  public CapacityBasedRetentionPolicy(long capacityBytes) {
    this.capacityBytes = capacityBytes;
  }

  @Override
  public com.ms.silverking.cloud.dht.ValueRetentionPolicy.ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
  }

  @Override
  public boolean considersStoredLength() {
    return true;
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

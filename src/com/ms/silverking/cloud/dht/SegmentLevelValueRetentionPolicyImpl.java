package com.ms.silverking.cloud.dht;

/**
 * Base class for use by retention policies that consider a (per server) capacity limit.
 */
public abstract class SegmentLevelValueRetentionPolicyImpl<S extends ValueRetentionState>
    extends ValueRetentionPolicyImpl<S> {
  protected final long capacityBytes;

  public SegmentLevelValueRetentionPolicyImpl(long capacityBytes) {
    this.capacityBytes = capacityBytes;
  }

  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
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

    SegmentLevelValueRetentionPolicyImpl other;

    other = (SegmentLevelValueRetentionPolicyImpl) o;
    return capacityBytes == other.capacityBytes;
  }

  public abstract boolean retains(int segmentNumber, int segmentSize, S valueRetentionState, long curTimeNanos);
}

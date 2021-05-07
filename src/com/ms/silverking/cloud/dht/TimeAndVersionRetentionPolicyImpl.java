package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.Pair;

public class TimeAndVersionRetentionPolicyImpl extends KeyLevelValueRetentionPolicyImpl<TimeAndVersionRetentionState> {
  private final TimeAndVersionRetentionPolicy policy;

  public TimeAndVersionRetentionPolicyImpl(TimeAndVersionRetentionPolicy policy) {
    this.policy = policy;
  }

  @Override
  public int hashCode() {
    return policy.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (this.getClass() != o.getClass()) {
      return false;
    }

    TimeAndVersionRetentionPolicyImpl other;

    other = (TimeAndVersionRetentionPolicyImpl) o;
    return policy.equals(other.policy);
  }

  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      TimeAndVersionRetentionState timeRetentionState, long curTimeNanos, int storedLength) {
    int totalVersions;
    long mostRecentCreationTimeNanos;
    long spanEndTimeNanos;
    Pair<Integer, Long> vData;
    long deltaNanos;

    vData = timeRetentionState.processValue(key, creationTimeNanos);
    totalVersions = vData.getV1();
    mostRecentCreationTimeNanos = vData.getV2();
    if (policy.getMode() == TimeAndVersionRetentionPolicy.Mode.wallClock) {
      spanEndTimeNanos = SystemTimeUtil.skSystemTimeSource.absTimeNanos();
    } else {
      spanEndTimeNanos = mostRecentCreationTimeNanos;
    }
    deltaNanos = spanEndTimeNanos - creationTimeNanos;
    //System.out.printf("%s %d %d %d\t%d\t%d\t%s\n", key, version, creationTimeNanos, spanEndTimeNanos,
    // totalVersions, deltaNanos, totalVersions > 1 ? "_G_" : "_NG_");
    return totalVersions <= policy.getMinVersions() || deltaNanos <= policy.getTimeSpanNanos();
  }

  @Override
  public TimeAndVersionRetentionState createInitialState() {
    return new TimeAndVersionRetentionState();
  }

  @Override
  public boolean considersStoredLength() {
    return false;
  }

  @Override
  public boolean considersInvalidations() {
    return true;
  }
}

package com.ms.silverking.cloud.dht.daemon.storage.retention;

import com.ms.silverking.cloud.dht.ValidOrTimeAndVersionRetentionPolicy;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.text.ObjectDefParser2;

@OmitGeneration
public class ValidOrTimeAndVersionRetentionPolicyImpl
    extends KeyLevelValueRetentionPolicyImpl<TimeAndVersionRetentionState> {
  private final ValidOrTimeAndVersionRetentionPolicy policy;

  public ValidOrTimeAndVersionRetentionPolicyImpl(ValidOrTimeAndVersionRetentionPolicy policy) {
    this.policy = policy;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
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

    ValidOrTimeAndVersionRetentionPolicyImpl other;

    other = (ValidOrTimeAndVersionRetentionPolicyImpl) o;
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
    if (policy.getMode() == ValidOrTimeAndVersionRetentionPolicy.Mode.wallClock) {
      spanEndTimeNanos = SystemTimeUtil.skSystemTimeSource.absTimeNanos();
    } else {
      spanEndTimeNanos = mostRecentCreationTimeNanos;
    }
    deltaNanos = spanEndTimeNanos - creationTimeNanos;
    //System.out.printf("%s %d %s %d %d\t%d\t%d\t%s\n", key, version, invalidated, creationTimeNanos,
    // spanEndTimeNanos, totalVersions, deltaNanos, totalVersions > 1 ? "_G_" : "_NG_");
    return (totalVersions <= 1 && !invalidated) // retain most recent value, if it's valid; for all other values, use
        // time and version retention policy
        || totalVersions <= policy.getMinVersions() || deltaNanos <= policy.getTimeSpanNanos();
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

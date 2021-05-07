package com.ms.silverking.cloud.dht;

import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

public class InvalidatedRetentionPolicyImpl extends KeyLevelValueRetentionPolicyImpl<InvalidatedRetentionState> {
  private final InvalidatedRetentionPolicy policy;

  static {
    ObjectDefParser2.addParserWithExclusions(InvalidatedRetentionPolicyImpl.class, null, FieldsRequirement.ALLOW_INCOMPLETE,
        null);
  }

  @OmitGeneration
  public InvalidatedRetentionPolicyImpl(InvalidatedRetentionPolicy policy) {
    this.policy = policy;
  }

  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      InvalidatedRetentionState invalidatedRetentionState, long curTimeNanos, int storedLength) {
    if (!invalidated) {
      if (invalidatedRetentionState.isInvalidated(key)) {
        invalidated = true;
      }
    } else {
      invalidatedRetentionState.setInvalidated(key);
    }
    if (invalidated) {
      long invalidatedRetentionIntervalNanos;

      invalidatedRetentionIntervalNanos = TimeUnit.NANOSECONDS.convert(policy.getInvalidatedRetentionIntervalSeconds(),
          TimeUnit.SECONDS);
      return creationTimeNanos + invalidatedRetentionIntervalNanos > curTimeNanos;
    } else {
      return true;
    }
  }

  @Override
  public InvalidatedRetentionState createInitialState() {
    return new InvalidatedRetentionState();
  }

  @Override
  public boolean considersStoredLength() {
    return false;
  }

  @Override
  public boolean considersInvalidations() {
    return true;
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

    InvalidatedRetentionPolicyImpl other;

    other = (InvalidatedRetentionPolicyImpl) o;
    return policy.equals(other.policy);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static InvalidatedRetentionPolicyImpl parse(String def) {
    return ObjectDefParser2.parse(InvalidatedRetentionPolicyImpl.class, def);
  }

  public static void main(String[] args) {
    String def;
    InvalidatedRetentionPolicyImpl irp;
    InvalidatedRetentionPolicyImpl irp2;

    irp = new InvalidatedRetentionPolicyImpl(new InvalidatedRetentionPolicy(10));
    def = irp.toString();
    System.out.println(def);
    irp2 = parse(def);
    System.out.println(irp2);
    irp2 = parse("invalidatedRetentionIntervalSeconds=10");
    System.out.println(irp2);

    InvalidatedRetentionState invalidatedRetentionState;
    long creationTimeNanos;
    long curTimeNanos;

    invalidatedRetentionState = new InvalidatedRetentionState();
    curTimeNanos = SystemTimeUtil.skSystemTimeSource.absTimeNanos();
    creationTimeNanos = curTimeNanos;
    System.out.println(
        irp.retains(new SimpleKey(0, 1), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos, 0));
    System.out.println(
        irp.retains(new SimpleKey(0, 2), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos, 0));
    creationTimeNanos = curTimeNanos - 100 * 1000000000L;
    System.out.println(
        irp.retains(new SimpleKey(0, 3), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos, 0));
    System.out.println(
        irp.retains(new SimpleKey(0, 4), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos, 0));

    System.out.println();
    creationTimeNanos = curTimeNanos - 200 * 1000000000L;
    System.out.println(
        irp.retains(new SimpleKey(0, 1), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos, 0));
    System.out.println(
        irp.retains(new SimpleKey(0, 2), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos, 0));
    System.out.println(
        irp.retains(new SimpleKey(0, 3), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos, 0));
    System.out.println(
        irp.retains(new SimpleKey(0, 4), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos, 0));
  }
}

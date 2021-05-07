package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.KeyLevelValueRetentionPolicyImpl;
import com.ms.silverking.cloud.dht.common.DHTKey;

/**
 * Only used in server side (within package scope)
 */
class InternalPurgeKeyRetentionPolicyImpl extends KeyLevelValueRetentionPolicyImpl<InternalPurgeKeyRetentionState> {
  private final DHTKey keyToPurge;
  private final long purgeBeforeCreationTimeNanos; // inclusive

  InternalPurgeKeyRetentionPolicyImpl(DHTKey keyToPurge, long purgeBeforeCreationTimeNanos) {
    this.keyToPurge = keyToPurge;
    this.purgeBeforeCreationTimeNanos = purgeBeforeCreationTimeNanos;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      InternalPurgeKeyRetentionState state, long curTimeNanos, int storedLength) {
    if (keyToPurge.equals(key) && creationTimeNanos <= purgeBeforeCreationTimeNanos) {
      state.keyPurged(creationTimeNanos, version);
      return false;
    } else {
      return true;
    }
  }

  @Override
  public boolean considersStoredLength() {
    return false;
  }

  @Override
  public boolean considersInvalidations() {
    return true;
  }

  // Not allowed as this policy is used internally
  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
  }

  // Not allowed as this policy is used internally
  @Override
  public InternalPurgeKeyRetentionState createInitialState() {
    throw new IllegalStateException(
        "Illegal call path: InternalPurgeKeyRetentionPolicy::createInitialState() is not allowed; This policy is for "
            + "internal use");
  }
}

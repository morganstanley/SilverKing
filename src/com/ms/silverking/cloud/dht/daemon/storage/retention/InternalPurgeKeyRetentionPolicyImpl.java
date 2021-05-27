package com.ms.silverking.cloud.dht.daemon.storage.retention;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;

/**
 * Used internally in server, not as the result of any user-specified policy
 */
@OmitGeneration
public class InternalPurgeKeyRetentionPolicyImpl extends KeyLevelValueRetentionPolicyImpl<InternalPurgeKeyRetentionState> {
  private final DHTKey keyToPurge;
  private final long purgeBeforeCreationTimeNanos; // inclusive

  public InternalPurgeKeyRetentionPolicyImpl(DHTKey keyToPurge, long purgeBeforeCreationTimeNanos) {
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

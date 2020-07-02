package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;

import static com.ms.silverking.cloud.dht.ValueRetentionPolicy.ImplementationType.SingleReverseSegmentWalk;

/**
 * Only used in server side (within package scope)
 */
class InternalPurgeKeyRetentionPolicy implements ValueRetentionPolicy<InternalPurgeKeyRetentionState> {
  private final DHTKey keyToPurge;
  private final long purgeBeforeCreationTimeNanos; // inclusive

  InternalPurgeKeyRetentionPolicy(DHTKey keyToPurge, long purgeBeforeCreationTimeNanos) {
    this.keyToPurge = keyToPurge;
    this.purgeBeforeCreationTimeNanos = purgeBeforeCreationTimeNanos;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      InternalPurgeKeyRetentionState state, long curTimeNanos, long storedLength) {
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

  // Not allowed as this policy is used internally
  @Override
  public ImplementationType getImplementationType() {
    return SingleReverseSegmentWalk;
  }

  // Not allowed as this policy is used internally
  @Override
  public InternalPurgeKeyRetentionState createInitialState(PutTrigger putTrigger, RetrieveTrigger retrieveTrigger) {
    throw new IllegalStateException(
        "Illegal call path: InternalPurgeKeyRetentionPolicy::createInitialState() is not allowed; This policy is for " +
            "internal use");
  }
}

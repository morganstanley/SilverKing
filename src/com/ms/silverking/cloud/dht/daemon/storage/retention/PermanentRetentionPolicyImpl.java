package com.ms.silverking.cloud.dht.daemon.storage.retention;

import com.ms.silverking.cloud.dht.common.DHTKey;

public class PermanentRetentionPolicyImpl extends FixedRetentionPolicyImpl {
  public PermanentRetentionPolicyImpl() {

  }

  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.RetainAll;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      EmptyValueRetentionState valueRetentionState, long curTimeNanos, int storedLength) {
    return true;
  }

  @Override
  public boolean considersStoredLength() {
    return false;
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }
}

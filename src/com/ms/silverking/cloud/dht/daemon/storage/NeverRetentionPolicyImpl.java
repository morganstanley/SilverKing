package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.FixedRetentionPolicyImpl;
import com.ms.silverking.cloud.dht.ValueRetentionState;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.text.ObjectDefParser2;

public class NeverRetentionPolicyImpl extends FixedRetentionPolicyImpl {
  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      ValueRetentionState.Empty state, long curTimeNanos, int storedLength) {
    return false;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}

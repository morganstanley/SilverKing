package com.ms.silverking.cloud.dht.daemon.storage.retention;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.text.ObjectDefParser2;

@OmitGeneration
public class NeverRetentionPolicyImpl extends FixedRetentionPolicyImpl {
  @Override
  public ImplementationType getImplementationType() {
    return ImplementationType.SingleReverseSegmentWalk;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      EmptyValueRetentionState state, long curTimeNanos, int storedLength) {
    return false;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}

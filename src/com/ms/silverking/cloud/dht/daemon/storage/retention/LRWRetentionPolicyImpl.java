package com.ms.silverking.cloud.dht.daemon.storage.retention;

import com.ms.silverking.cloud.dht.LRWRetentionPolicy;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;

@OmitGeneration
public class LRWRetentionPolicyImpl extends SegmentLevelValueRetentionPolicyImpl<LRWRetentionState> {
  public LRWRetentionPolicyImpl(LRWRetentionPolicy policy) {
    super(policy.getCapacityBytes());
  }

  @Override
  public boolean retains(int segmentNumber, int segmentSize, LRWRetentionState lrwRetentionState, long curTimeNanos) {
    if (lrwRetentionState.getBytesRetained() + segmentSize <= capacityBytes) {
      lrwRetentionState.addBytesRetained(segmentSize);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public LRWRetentionState createInitialState() {
    return new LRWRetentionState();
  }
}
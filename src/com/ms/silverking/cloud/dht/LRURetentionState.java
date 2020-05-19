package com.ms.silverking.cloud.dht;

import java.util.Map;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.numeric.MutableInteger;

/**
 * Per-key LRU state
 */
class LRURetentionState extends CapacityBasedRetentionState {
  private final Map<DHTKey, MutableInteger> retentionMap;

  @OmitGeneration
  public LRURetentionState(Map<DHTKey, MutableInteger> retentionMap) {
    this.retentionMap = retentionMap;
  }

  public boolean retains(DHTKey key) {
    MutableInteger remainingRetention;

    remainingRetention = retentionMap.get(key);
    if (remainingRetention != null && remainingRetention.getValue() > 0) {
      remainingRetention.decrement();
      return true;
    } else {
      return false;
    }
  }
}

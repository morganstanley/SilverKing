package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Least-recently written value retention policy. LRW is per key.
 */
public class LRWRetentionPolicy extends CapacityBasedRetentionPolicy<LRWRetentionState> {
  static final LRWRetentionPolicy template = new LRWRetentionPolicy(0);

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public LRWRetentionPolicy(long capacityBytes) {
    super(capacityBytes);
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      LRWRetentionState lrwRetentionState, long curTimeNanos, long storedLength) {
    //System.out.printf("\t%d\t%d\t%d\n", lrwRetentionState.getBytesRetained(), storedLength, capacityBytes);
    if (lrwRetentionState.getBytesRetained() + storedLength <= capacityBytes) {
      lrwRetentionState.addBytesRetained(storedLength);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public LRWRetentionState createInitialState(PutTrigger putTrigger, RetrieveTrigger retrieveTrigger) {
    return new LRWRetentionState();
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static LRWRetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(LRWRetentionPolicy.class, def);
  }
}

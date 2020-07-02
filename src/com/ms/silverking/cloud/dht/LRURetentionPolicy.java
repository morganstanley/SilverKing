package com.ms.silverking.cloud.dht;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUKeyedInfo;
import com.ms.silverking.cloud.dht.daemon.storage.serverside.LRUStateProvider;
import com.ms.silverking.cloud.dht.serverside.PutTrigger;
import com.ms.silverking.cloud.dht.serverside.RetrieveTrigger;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.MutableInteger;
import com.ms.silverking.text.ObjectDefParser2;

import java.util.Map;
import java.util.Queue;

/**
 * Simple LRU value retention policy. LRU is per key.
 */
public class LRURetentionPolicy extends CapacityBasedRetentionPolicy<LRURetentionState> {
  private final int maxVersions;

  static final LRURetentionPolicy template = new LRURetentionPolicy(0, 1);

  static {
    ObjectDefParser2.addParser(template);
  }

  @OmitGeneration
  public LRURetentionPolicy() {
    this(0, 0);
  }

  @OmitGeneration
  public LRURetentionPolicy(long capacityBytes, int maxVersions) {
    super(capacityBytes);
    this.maxVersions = maxVersions;
  }

  @Override
  public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated,
      LRURetentionState lruRetentionState, long curTimeNanos, long storedLength) {
    return lruRetentionState.retains(key);
  }

  @Override
  public LRURetentionState createInitialState(PutTrigger putTrigger, RetrieveTrigger retrieveTrigger) {
    LRUStateProvider lruState;

    //System.out.printf("createInitialState\n");
    if (putTrigger == null) {
      Log.warning("LRURetentionPolicy has no put trigger");
    }
    if (retrieveTrigger == null) {
      Log.warning("LRURetentionPolicy has no retrieve trigger");
    }
    lruState = (LRUStateProvider) retrieveTrigger;
    return new LRURetentionState(createRetentionMap(lruState.getLRUList()));
  }

  /**
   * Returns map of key=>number of versions to retain
   *
   * @param lruList
   * @return
   */
  private Map<DHTKey, MutableInteger> createRetentionMap(Queue<LRUKeyedInfo> lruList) {
    long bytesRetained;
    ImmutableMap.Builder<DHTKey, MutableInteger> retentionMap;

    //System.out.printf("createRetentionMap %d\n", lruList.size());
    retentionMap = ImmutableMap.builder();
    bytesRetained = 0;
    // List is sorted in ascending access time order
    // Walk the list backwards to retain the most recently accessed first
    while (lruList.peek() != null && bytesRetained < capacityBytes) {
      LRUKeyedInfo lruKeyedInfo;

      //System.out.printf("%d\t%d\n", i, bytesRetained);
      // We know that remove() is safe here since we peek() above.
      lruKeyedInfo = lruList.remove();
      if (bytesRetained < capacityBytes) {
        bytesRetained += lruKeyedInfo.getSize();
        retentionMap.put(lruKeyedInfo.getKey(), new MutableInteger(maxVersions));
      }
    }
    return retentionMap.build();
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static LRURetentionPolicy parse(String def) {
    return ObjectDefParser2.parse(LRURetentionPolicy.class, def);
  }
}

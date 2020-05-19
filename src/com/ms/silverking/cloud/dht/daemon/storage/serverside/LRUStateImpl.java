package com.ms.silverking.cloud.dht.daemon.storage.serverside;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.FibPriorityQueue;
import com.ms.silverking.time.AbsNanosTimeSource;

public class LRUStateImpl implements LRUStateProvider {
  private AbsNanosTimeSource timeSource;
  private final ConcurrentMap<DHTKey, LRUInfo> lruInfoMap;

  public LRUStateImpl(AbsNanosTimeSource timeSource) {
    if (timeSource == null) {
      this.timeSource = SystemTimeUtil.skSystemTimeSource;
    } else {
      this.timeSource = timeSource;
    }
    this.lruInfoMap = new ConcurrentHashMap<>();
  }

  public LRUStateImpl(AbsNanosTimeSource timeSource, Map<DHTKey, LRUInfo> lruInfoMap) {
    this(timeSource);
    this.lruInfoMap.putAll(lruInfoMap);
  }

  public void markRead(DHTKey key) {
    LRUInfo lruInfo;

    lruInfo = lruInfoMap.get(key);
    if (lruInfo != null) {
      lruInfo.updateAccessTime(timeSource.absTimeNanos());
    } else {
      // Ignore for now
    }
  }

  public void markPut(DHTKey key, int compressedSize) {
    LRUInfo lruInfo;

    lruInfo = lruInfoMap.get(key);
    if (lruInfo == null) {
      lruInfo = new LRUInfo(timeSource.absTimeNanos(), compressedSize);
      // Create a copy of the key to avoid referencing the incoming message
      lruInfoMap.put(new SimpleKey(key), lruInfo);
    } else {
      lruInfo.update(timeSource.absTimeNanos(), compressedSize);
    }
  }

  @Override
  public Queue<LRUKeyedInfo> getLRUList() {
    FibPriorityQueue<LRUKeyedInfo> lruList;

    lruList = new FibPriorityQueue<>();
    for (Map.Entry<DHTKey, LRUInfo> entry : lruInfoMap.entrySet()) {
      LRUKeyedInfo ki;

      ki = new LRUKeyedInfo(entry.getKey(), entry.getValue());
      // Use add() instead of offer() here because we want to fail loudly
      // if we don't have capacity to add.
      lruList.add(ki, (double) (-1 * ki.getAccessTime()));
    }
    return lruList;
  }

}

package com.ms.silverking.cloud.dht.daemon.storage.serverside;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;

public class LRUKeyedInfo {
  private final DHTKey key;
  private final long accessTime;
  private final int size;

  public LRUKeyedInfo(DHTKey key, long accessTime, int size) {
    this.key = key;
    this.accessTime = accessTime;
    this.size = size;
  }

  public LRUKeyedInfo(DHTKey key, LRUInfo info) {
    this(key, info.getAccessTime(), info.getSize());
  }

  public DHTKey getKey() {
    return key;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public int getSize() {
    return size;
  }

  @Override
  public String toString() {
    return String.format("%s:%d:%d", KeyUtil.keyToString(key), accessTime, size);
  }
}

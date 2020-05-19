package com.ms.silverking.cloud.dht.daemon.storage.serverside;

public class LRUInfo {
  private long accessTime;
  private int size;

  public LRUInfo(long accessTime, int size) {
    this.accessTime = accessTime;
    this.size = size;
  }

  public void updateAccessTime(long accessTime) {
    this.accessTime = accessTime;
  }

  public void update(long accessTime, int size) {
    this.accessTime = accessTime;
    this.size = size;
  }

  public long getAccessTime() {
    return accessTime;
  }

  public int getSize() {
    return size;
  }

  @Override
  public String toString() {
    return String.format("%d:%d", accessTime, size);
  }
}

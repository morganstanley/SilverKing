package com.ms.silverking.cloud.dht;

/**
 * Type of value storage.
 */
public enum StorageType {
  RAM, FILE, FILE_SYNC;

  public boolean isFileBased() {
    return this != RAM;
  }
}

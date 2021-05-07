package com.ms.silverking.collection.cuckoo;

/**
 * Pairs the key with the value that the key maps to in the hash table.
 */
class IntKeyIntEntry {
  private final int key;
  private final int value;

  IntKeyIntEntry(int key, int value) {
    this.key = key;
    this.value = value;
  }

  public int getKey() {
    return key;
  }

  public int getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return Integer.hashCode(key);
  }

  @Override
  public boolean equals(Object o) {
    IntKeyIntEntry  other;

    other = (IntKeyIntEntry)o;
    return key == other.key && value == other.value;
  }

  @Override
  public String toString() {
    return String.format("%d:%d", key, value);
  }
}
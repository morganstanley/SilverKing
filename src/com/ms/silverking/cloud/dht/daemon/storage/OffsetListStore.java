package com.ms.silverking.cloud.dht.daemon.storage;

/**
 * Group of offset lists for a given segment.
 */
public interface OffsetListStore {
  public static final boolean debug = false;

  /**
   * Returns a new OffsetList. Only valid for non-persisted segments.
   *
   * @return a new offset list
   */
  public OffsetList newOffsetList();

  /**
   * Returns an existing offset list
   *
   * @param index index of offset list
   * @return offset list at given index
   */
  public OffsetList getOffsetList(int index);
}

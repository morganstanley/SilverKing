package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Comparator;

public class KeyAndVersionChecksumSegmentNumberComparator implements Comparator<KeyAndVersionChecksum> {
  private final int order;

  public static final KeyAndVersionChecksumSegmentNumberComparator ascendingSort =
      new KeyAndVersionChecksumSegmentNumberComparator(
      1);
  public static final KeyAndVersionChecksumSegmentNumberComparator descendingSort =
      new KeyAndVersionChecksumSegmentNumberComparator(
      -1);

  private KeyAndVersionChecksumSegmentNumberComparator(int order) {
    this.order = order;
  }

  @Override
  public int compare(KeyAndVersionChecksum k1, KeyAndVersionChecksum k2) {
    return order * Long.compare(k1.getSegmentNumber(), k2.getSegmentNumber());
  }
}

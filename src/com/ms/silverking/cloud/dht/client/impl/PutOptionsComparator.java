package com.ms.silverking.cloud.dht.client.impl;

import java.util.Comparator;

import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.util.ArrayUtil;

/**
 * This class is intended to impose an ordering (indirectly) on ActivePutOperationImpl instances that are being
 * ordered for sending. We want to send earlier versions first. Other ordering is arbitrary but intended to
 * group compatible operations together.
 */
class PutOptionsComparator implements Comparator<PutOptions> {
  public static final PutOptionsComparator instance = new PutOptionsComparator();

  @Override
  public int compare(PutOptions o1, PutOptions o2) {
    int c;

    if (o1.getVersion() < o2.getVersion()) {
      return -1;
    } else if (o1.getVersion() > o2.getVersion()) {
      return 1;
    } else {
      c = o1.getCompression().compareTo(o2.getCompression());
      if (c != 0) {
        return c;
      } else {
        c = o1.getChecksumType().compareTo(o2.getChecksumType());
        if (c != 0) {
          return c;
        } else {
          c = Boolean.compare(o1.getChecksumCompressedValues(), o2.getChecksumCompressedValues());
          if (c != 0) {
            return c;
          } else {
            return ArrayUtil.compareSignedForOrdering(o1.getUserData(), o2.getUserData());
          }
        }
      }
    }
  }
}

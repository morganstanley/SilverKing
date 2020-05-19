package com.ms.silverking.net;

import java.net.InetAddress;
import java.util.Comparator;

import com.ms.silverking.util.ArrayUtil;

public class InetAddressComparator implements Comparator<InetAddress> {
  public static final InetAddressComparator instance = new InetAddressComparator();

  public InetAddressComparator() {
  }

  @Override
  public int compare(InetAddress a1, InetAddress a2) {
    return ArrayUtil.compareSigned(a1.getAddress(), a2.getAddress());
  }
}

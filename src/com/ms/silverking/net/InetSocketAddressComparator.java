package com.ms.silverking.net;

import java.net.InetSocketAddress;
import java.util.Comparator;

import com.ms.silverking.util.ArrayUtil;

public class InetSocketAddressComparator implements Comparator<InetSocketAddress> {
  public static final InetSocketAddressComparator instance = new InetSocketAddressComparator();

  public InetSocketAddressComparator() {
  }

  @Override
  public int compare(InetSocketAddress a1, InetSocketAddress a2) {
    int result;

    result = ArrayUtil.compareSigned(a1.getAddress().getAddress(), a2.getAddress().getAddress());
    if (result != 0) {
      return result;
    } else {
      if (a1.getPort() < a2.getPort()) {
        return -1;
      } else if (a1.getPort() > a2.getPort()) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}

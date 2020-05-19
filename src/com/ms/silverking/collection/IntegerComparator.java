package com.ms.silverking.collection;

import java.util.Comparator;

public class IntegerComparator implements Comparator<Integer> {
  public static final IntegerComparator instance = new IntegerComparator();

  public IntegerComparator() {
  }

  @Override
  public int compare(Integer o1, Integer o2) {
    return o1.compareTo(o2);
  }
}

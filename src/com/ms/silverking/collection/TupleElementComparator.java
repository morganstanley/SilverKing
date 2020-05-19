package com.ms.silverking.collection;

import java.util.Comparator;

public class TupleElementComparator implements Comparator<TupleBase> {

  private final int elementIndex;

  private final Comparator elementComparator;

  public TupleElementComparator(int elementIndex, Comparator elementComparator) {

    this.elementComparator = elementComparator;

    this.elementIndex = elementIndex;

  }

  @Override

  public int compare(TupleBase o1, TupleBase o2) {

    return elementComparator.compare(o1.getElement(elementIndex), o2.getElement(elementIndex));

  }

}


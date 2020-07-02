package com.ms.silverking.numeric;

import java.io.Serializable;

public class Priority implements Comparable<Priority>, Serializable {
  protected int priority;

  public Priority(int priority) {
    this.priority = priority;
  }

  public Priority(Priority priority) {
    this.priority = priority.priority;
  }

  public boolean lessThan(Priority other) {
    return priority < other.priority;
  }

  public boolean lessThanEq(Priority other) {
    return priority <= other.priority;
  }

  public boolean greaterThan(Priority other) {
    return priority > other.priority;
  }

  public boolean greaterThanEq(Priority other) {
    return priority >= other.priority;
  }

  public int compareTo(Priority other) {
    if (priority < other.priority) {
      return -1;
    } else if (priority > other.priority) {
      return 1;
    } else {
      return 0;
    }
  }

  public int hashCode() {
    return priority;
  }

  public boolean equals(Object other) {
    Priority otherP;

    otherP = (Priority) other;
    return priority == otherP.priority;
  }

  public int toInt() {
    return priority;
  }

  public String toString() {
    return new Integer(priority).toString();
  }
}


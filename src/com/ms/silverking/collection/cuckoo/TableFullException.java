package com.ms.silverking.collection.cuckoo;

public class TableFullException extends RuntimeException {
  public TableFullException() {
    super();
  }

  public TableFullException(String message) {
    super(message);
  }
}

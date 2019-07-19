package com.ms.silverking.cloud.dht.collection;

public class TableFullException extends RuntimeException {
    public TableFullException() {
        super();
    }

    public TableFullException(String message) {
        super(message);
    }
}

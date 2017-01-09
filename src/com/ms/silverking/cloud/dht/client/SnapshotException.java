package com.ms.silverking.cloud.dht.client;

public abstract class SnapshotException extends OperationException {
    public SnapshotException() {
        super();
    }

    public SnapshotException(String message) {
        super(message);
    }

    public SnapshotException(Throwable cause) {
        super(cause);
    }

    public SnapshotException(String message, Throwable cause) {
        super(message, cause);
    }
}

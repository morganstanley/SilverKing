package com.ms.silverking.cloud.dht.client;

public abstract class SyncRequestException extends OperationException {
    public SyncRequestException() {
        super();
    }

    public SyncRequestException(String message) {
        super(message);
    }

    public SyncRequestException(Throwable cause) {
        super(cause);
    }

    public SyncRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String getDetailedFailureMessage() {
        return "";
    }
}

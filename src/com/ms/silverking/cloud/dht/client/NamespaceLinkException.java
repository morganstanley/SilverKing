package com.ms.silverking.cloud.dht.client;

public class NamespaceLinkException extends OperationException {
    public NamespaceLinkException() {
        super();
    }

    public NamespaceLinkException(String message) {
        super(message);
    }

    public NamespaceLinkException(Throwable cause) {
        super(cause);
    }

    public NamespaceLinkException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String getDetailedFailureMessage() {
        return super.getMessage();
    }
}

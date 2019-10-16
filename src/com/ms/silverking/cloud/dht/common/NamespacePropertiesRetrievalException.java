package com.ms.silverking.cloud.dht.common;

public class NamespacePropertiesRetrievalException extends Exception {
    public NamespacePropertiesRetrievalException() {
    }

    public NamespacePropertiesRetrievalException(String message) {
        super(message);
    }

    public NamespacePropertiesRetrievalException(Throwable cause) {
        super(cause);
    }

    public NamespacePropertiesRetrievalException(String message, Throwable cause) {
        super(message, cause);
    }

    public NamespacePropertiesRetrievalException(String message, Throwable cause,
                                                 boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

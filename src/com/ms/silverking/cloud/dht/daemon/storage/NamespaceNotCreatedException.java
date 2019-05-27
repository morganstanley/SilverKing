package com.ms.silverking.cloud.dht.daemon.storage;

/**
 * Indicates that an operation that requires a prior explicit namespace creation cannot 
 * find the required namespace. 
 */
public class NamespaceNotCreatedException extends RuntimeException {
    public NamespaceNotCreatedException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public NamespaceNotCreatedException(String message, Throwable cause) {
        super(message, cause);
    }

    public NamespaceNotCreatedException(String message) {
        super(message);
    }
}

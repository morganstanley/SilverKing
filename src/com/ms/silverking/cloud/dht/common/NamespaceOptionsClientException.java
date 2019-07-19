package com.ms.silverking.cloud.dht.common;

/**
 * Thrown when an unexpected exception occurs while trying to retrieve NamespaceOptions.
 */
public class NamespaceOptionsClientException extends RuntimeException {
    public NamespaceOptionsClientException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public NamespaceOptionsClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public NamespaceOptionsClientException(String message) {
        super(message);
    }
}

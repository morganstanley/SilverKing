package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

/**
 * Thrown when namespace creation fails.
 */
@NonVirtual
public class NamespaceCreationException extends OperationException {
    public NamespaceCreationException() {
        super();
    }

    public NamespaceCreationException(String message) {
        super(message);
    }

    public NamespaceCreationException(Throwable cause) {
        super(cause);
    }

    public NamespaceCreationException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String getDetailedFailureMessage() {
        return super.getMessage();
    }
}

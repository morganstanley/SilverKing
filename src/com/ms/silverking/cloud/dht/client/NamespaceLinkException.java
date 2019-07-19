package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.NonVirtual;

@NonVirtual
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

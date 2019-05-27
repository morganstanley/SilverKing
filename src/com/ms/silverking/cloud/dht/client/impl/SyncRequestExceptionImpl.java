package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.SyncRequestException;

class SyncRequestExceptionImpl extends SyncRequestException {
    public SyncRequestExceptionImpl(String message, Throwable cause) {
        super(message, cause);
    }

    public SyncRequestExceptionImpl(String message) {
        super(message);
    }

    public SyncRequestExceptionImpl(Throwable cause) {
        super(cause);
    }

    public SyncRequestExceptionImpl() {
        super();
    }

    @Override
    public String getDetailedFailureMessage() {
        return super.getMessage();
    }
}

package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.SnapshotException;

class SnapshotExceptionImpl extends SnapshotException {
    public SnapshotExceptionImpl(String message, Throwable cause) {
        super(message, cause);
    }

    public SnapshotExceptionImpl(String message) {
        super(message);
    }

    public SnapshotExceptionImpl(Throwable cause) {
        super(cause);
    }

    public SnapshotExceptionImpl() {
        super();
    }

    @Override
    public String getDetailedFailureMessage() {
        return super.getMessage();
    }
}

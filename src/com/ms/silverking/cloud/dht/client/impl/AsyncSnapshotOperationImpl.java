package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.AsyncSnapshot;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.common.Context;

class AsyncSnapshotOperationImpl extends AsyncVersionedBasicOperationImpl implements AsyncSnapshot {
    public AsyncSnapshotOperationImpl(VersionedBasicNamespaceOperation versionedOperation, Context context,
            long curTime, byte[] originator) {
        super(versionedOperation, context, curTime, originator);
    }
    
    @Override
    protected void throwFailedException() throws OperationException {
        throw new SnapshotExceptionImpl(getFailureCause().toString());
    }
}

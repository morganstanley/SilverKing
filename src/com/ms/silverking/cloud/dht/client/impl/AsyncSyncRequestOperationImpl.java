package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.AsyncSyncRequest;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.common.Context;

class AsyncSyncRequestOperationImpl extends AsyncVersionedBasicOperationImpl implements AsyncSyncRequest {
    public AsyncSyncRequestOperationImpl(VersionedBasicNamespaceOperation versionedOperation, Context context,
            long curTime, byte[] originator) {
        super(versionedOperation, context, curTime, originator);
    }
    
    @Override
    protected void throwFailedException() throws OperationException {
        throw new SyncRequestExceptionImpl(getFailureCause().toString());
    }
}

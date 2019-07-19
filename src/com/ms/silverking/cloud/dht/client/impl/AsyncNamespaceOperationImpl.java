package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.common.Context;

abstract class AsyncNamespaceOperationImpl extends AsyncOperationImpl {
    protected final Context    context;
    
    AsyncNamespaceOperationImpl(NamespaceOperation operation, Context context, long curTime, 
                                byte[] originator) {
        super(operation, curTime, originator);
        this.context = context;
    }
    
    public long getContext() {
        return context.contextAsLong();
    }
}

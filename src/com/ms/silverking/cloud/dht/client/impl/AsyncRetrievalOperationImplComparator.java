package com.ms.silverking.cloud.dht.client.impl;

import java.util.Comparator;

class AsyncRetrievalOperationImplComparator implements Comparator<AsyncRetrievalOperationImpl> {
    public static final AsyncRetrievalOperationImplComparator    instance = new AsyncRetrievalOperationImplComparator();
    
    @Override
    public int compare(AsyncRetrievalOperationImpl o1, AsyncRetrievalOperationImpl o2) {
        return RetrievalOptionsComparator.instance.compare(o1.retrievalOptions(), o2.retrievalOptions());
    }
}

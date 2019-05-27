package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;

public interface RetrievalOperationContainer extends OperationContainer {
    public InternalRetrievalOptions getRetrievalOptions();
}

package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.daemon.StorageReplicaProvider;
import com.ms.silverking.cloud.dht.net.ForwardingMode;


public class LooseConsistency implements StorageProtocol, RetrievalProtocol {
    private final StorageReplicaProvider   storageReplicaProvider;
    
    public LooseConsistency(StorageReplicaProvider storageNodeProvider) {
        this.storageReplicaProvider = storageNodeProvider;
    }

    //@Override
    public StorageOperation createStorageOperation(long timeout, PutOperationContainer putOperationContainer, ForwardingMode forwardingMode) {
        return new LooseConsistencyWrite(putOperationContainer, forwardingMode, timeout);
    }

    @Override
    public RetrievalOperation createRetrievalOperation(long timeout, RetrievalOperationContainer retrievalOperationContainer, ForwardingMode forwardingMode) {
        return new LooseConsistencyRead(timeout, retrievalOperationContainer, forwardingMode);
    }
    
    @Override
    public boolean sendResultsDuringStart() {
        return true;
    }
}

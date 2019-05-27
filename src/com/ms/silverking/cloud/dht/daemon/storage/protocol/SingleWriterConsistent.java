package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.daemon.StorageReplicaProvider;
import com.ms.silverking.cloud.dht.net.ForwardingMode;

public class SingleWriterConsistent implements StorageProtocol, RetrievalProtocol {
    private final StorageReplicaProvider   storageReplicaProvider;
    
    public SingleWriterConsistent(StorageReplicaProvider storageNodeProvider) {
        this.storageReplicaProvider = storageNodeProvider;
    }

    @Override
    public RetrievalOperation createRetrievalOperation(long deadline,
            RetrievalOperationContainer retrievalOperationContainer, ForwardingMode forwardingMode) {
        return new SingleWriterConsistentRead(deadline, retrievalOperationContainer, forwardingMode);
    }

    @Override
    public StorageOperation createStorageOperation(long timeout, PutOperationContainer putOperationContainer,
            ForwardingMode forwardingMode) {
        return new SingleWriterConsistentWrite(timeout, putOperationContainer, forwardingMode);
    }
    
    @Override
    public boolean sendResultsDuringStart() {
        return false;
    }
}

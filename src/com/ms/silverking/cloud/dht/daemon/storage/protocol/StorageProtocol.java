package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.net.ForwardingMode;

/**
 * Storage protocol for distributed storage.
 */
public interface StorageProtocol {
    public StorageOperation createStorageOperation(long timeout, PutOperationContainer putOperationContainer, 
            ForwardingMode forwardingMode);
    public boolean sendResultsDuringStart();
}

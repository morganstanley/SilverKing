package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.net.ForwardingMode;

/**
 * Read operation for the LooseConsistency StorageProtocol.
 */
public class LooseConsistencyRead extends SimpleRetrievalOperation {
    LooseConsistencyRead(long deadline, RetrievalOperationContainer retrievalOperationContainer,
            ForwardingMode forwardingMode) {
        super(deadline, retrievalOperationContainer, forwardingMode);
    }
}

package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.net.ForwardingMode;

public class SingleWriterConsistentRead extends SimpleRetrievalOperation {
    SingleWriterConsistentRead(long deadline, RetrievalOperationContainer retrievalOperationContainer,
            ForwardingMode forwardingMode) {
        super(deadline, retrievalOperationContainer, forwardingMode);
    }
}

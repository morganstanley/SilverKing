package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.OperationOptions;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.SimpleTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;

class VersionedBasicNamespaceOperation extends NamespaceOperation {
    private final long  version;
    
    // FUTURE - get from ns etc. like put/get
    private static final OpTimeoutController    opTimeoutController = new SimpleTimeoutController(5, 2 * 60 * 1000); 
    
    VersionedBasicNamespaceOperation(ClientOpType opType, ClientNamespace namespace, long version) {
        super(opType, namespace, new OperationOptions(opTimeoutController, DHTConstants.noSecondaryTargets));
        this.version = version;
    }
    
    long getVersion() {
        return version;
    }

    @Override
    OpTimeoutController getTimeoutController() {
        return opTimeoutController;
    }
}

package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.OperationOptions;

/**
 * Operation specific to a namespace.
 */
abstract class NamespaceOperation extends Operation {
	protected final ClientNamespace	namespace;

    NamespaceOperation(ClientOpType opType, ClientNamespace namespace, OperationOptions options) {
        super(opType, options);
        this.namespace = namespace;
    }
    
	ClientNamespace getClientNamespace() {
	    return namespace;
	}
}

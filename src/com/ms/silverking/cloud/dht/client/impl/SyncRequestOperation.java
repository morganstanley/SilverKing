package com.ms.silverking.cloud.dht.client.impl;

class SyncRequestOperation extends VersionedBasicNamespaceOperation {
    SyncRequestOperation(ClientNamespace namespace, long version) {
        super(ClientOpType.SYNC_REQUEST, namespace, version);
    }
}

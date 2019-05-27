package com.ms.silverking.cloud.dht.client.impl;

class SnapshotOperation extends VersionedBasicNamespaceOperation {
    SnapshotOperation(ClientNamespace namespace, long version) {
        super(ClientOpType.SNAPSHOT, namespace, version);
    }
}

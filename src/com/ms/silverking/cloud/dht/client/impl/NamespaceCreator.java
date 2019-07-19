package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.common.Namespace;

public interface NamespaceCreator {
    public Namespace createNamespace(String namespace);
}

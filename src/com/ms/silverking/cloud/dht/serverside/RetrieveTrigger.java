package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.DHTKey;

public interface RetrieveTrigger extends Trigger {
    public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options);

}

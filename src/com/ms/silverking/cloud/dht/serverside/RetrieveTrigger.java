package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;
import java.util.Iterator;

import com.ms.silverking.cloud.dht.common.DHTKey;

public interface RetrieveTrigger extends Trigger {
    public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options);
    public Iterator<DHTKey>    keyIterator();
    public long getTotalKeys();
    public boolean subsumesStorage();
}

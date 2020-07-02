package com.ms.silverking.cloud.dht.serverside;

import com.ms.silverking.cloud.dht.common.DHTKey;

import java.nio.ByteBuffer;
import java.util.Iterator;

public interface RetrieveTrigger extends Trigger {
  public ByteBuffer retrieve(SSNamespaceStore nsStore, DHTKey key, SSRetrievalOptions options);

  public ByteBuffer[] retrieve(SSNamespaceStore nsStore, DHTKey[] keys, SSRetrievalOptions options);

  public Iterator<DHTKey> keyIterator();

  public long getTotalKeys();

  public boolean subsumesStorage();
}

package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;

interface ReadableSegment {
    //int size();
    ByteBuffer retrieve(DHTKey key, InternalRetrievalOptions options);
}

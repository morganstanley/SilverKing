package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.common.DHTKey;

interface WritableSegment {
    //int size();
    SegmentStorageResult put(DHTKey key, ByteBuffer value, StorageParameters storageParams, byte[] userData, 
                             NamespaceOptions nsOptions);
}

package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.client.serialization.BufferDestSerializer;
import com.ms.silverking.cloud.dht.common.DHTKey;


public class SerializedMD5KeyCreator<K> extends BaseKeyCreator<K> {
    //private final Map<String,DHTKey>        cachedKeys;
    private final BufferDestSerializer<K>   serializer;
    private final ArrayMD5KeyCreator        arrayMD5KeyCreator;
    
    //private static final int    cacheCapacity = 1024;
    //private static final int    cacheConcurrencyLevel = 8;
    
    // FUTURE think about allowing users to override
    
    public SerializedMD5KeyCreator(BufferDestSerializer<K> serializer) {
        super();
        this.serializer = serializer;
        arrayMD5KeyCreator = new ArrayMD5KeyCreator();
        //cachedKeys = new MapMaker().concurrencyLevel(cacheConcurrencyLevel).initialCapacity(cacheCapacity).makeMap();
        //cachedKeys = null;
        // FUTURE - could consider using cached keys
    }
    
    @Override
    public DHTKey createKey(K key) {
        return arrayMD5KeyCreator.createKey(serializer.serializeToBuffer(key).array());
    }
}

package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.common.DHTKey;

class KeyMaster {
    private final ConcurrentMap<DHTKey,KeyMeta>   metaMap;
    
    KeyMaster() {
        metaMap = new ConcurrentHashMap<>();
    }
    
    KeyMeta getKeyMeta(DHTKey key) {
        KeyMeta meta;
        
        meta = metaMap.get(key);
        if (meta == null) {
            KeyMeta oldEntry;
            
            meta = new KeyMeta();
            oldEntry = metaMap.putIfAbsent(key, meta);
            if (oldEntry != null) {
                meta = oldEntry;
            }
        }
        return meta;
    }
}

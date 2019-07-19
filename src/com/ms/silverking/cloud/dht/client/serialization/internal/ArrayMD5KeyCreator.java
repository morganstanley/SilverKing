package com.ms.silverking.cloud.dht.client.serialization.internal;

import java.util.Map;

import com.ms.silverking.cloud.dht.common.DHTKey;


public class ArrayMD5KeyCreator extends BaseKeyCreator<byte[]> {
    private static final int        noArrayThreshold = 32;
    
    private final Map<String,DHTKey>    cachedKeys;
    
    private static final int    cacheCapacity = 1024;
    private static final int    cacheConcurrencyLevel = 8;
    
    // FUTURE think about allowing users to override
    
    public ArrayMD5KeyCreator() {
        super();
        //cachedKeys = new MapMaker().concurrencyLevel(cacheConcurrencyLevel).initialCapacity(cacheCapacity).makeMap();
        cachedKeys = null;
        // FUTURE - could consider using cached keys
    }
    
    @Override
    public DHTKey createKey(byte[] key) {
        DHTKey  dhtKey;
        
        //dhtKey = cachedKeys.get(key);
        //if (dhtKey == null) {
            dhtKey = md5KeyDigest.computeKey(key);
            //cachedKeys.put(key, dhtKey);
        //}
        return dhtKey;
    }
    
    public static void main(String[] args) {
        ArrayMD5KeyCreator stringMD5KeyCreator;
        DHTKey      key;
        DHTKey[]    subKeys;
        int         numSubKeys;

        stringMD5KeyCreator = new ArrayMD5KeyCreator();
        key = stringMD5KeyCreator.createKey("GeorgeWashington".getBytes());
        numSubKeys = 5;
        subKeys = stringMD5KeyCreator.createSubKeys(key, numSubKeys);
        System.out.println(key);
        for (int i = 0; i < subKeys.length; i++) {
            System.out.printf("%d\t%s\n", i, subKeys[i]);
        }
    }
}

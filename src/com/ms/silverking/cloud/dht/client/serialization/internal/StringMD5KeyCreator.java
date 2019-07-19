package com.ms.silverking.cloud.dht.client.serialization.internal;

import java.nio.charset.Charset;
import java.util.Map;

import com.ms.silverking.cloud.dht.common.DHTKey;


public class StringMD5KeyCreator extends BaseKeyCreator<String> {
    private static final boolean    use8BitEncoding;
    private static final int        noArrayThreshold = 32;
    
    private final Map<String,DHTKey>    cachedKeys;
    
    private static final int    cacheCapacity = 1024;
    private static final int    cacheConcurrencyLevel = 8;
    
    // FUTURE think about allowing users to override
    
    static {
        use8BitEncoding = Charset.defaultCharset().name().equals("UTF-8");
    }
    
    public StringMD5KeyCreator() {
        super();
        //cachedKeys = new MapMaker().concurrencyLevel(cacheConcurrencyLevel).initialCapacity(cacheCapacity).makeMap();
        cachedKeys = null;
        // FUTURE - could consider using cached keys
    }
    
    @Override
    public DHTKey createKey(String key) {
        DHTKey  dhtKey;
        
        //dhtKey = cachedKeys.get(key);
        //if (dhtKey == null) {
            if (use8BitEncoding && key.length() < noArrayThreshold) {
                dhtKey = md5KeyDigest.computeKey(key);
            } else {
                dhtKey = md5KeyDigest.computeKey(key.getBytes());
            }
            //cachedKeys.put(key, dhtKey);
        //}
        return dhtKey;
    }
    
    public static void main(String[] args) {
        StringMD5KeyCreator stringMD5KeyCreator;
        DHTKey      key;
        DHTKey[]    subKeys;
        int         numSubKeys;

        stringMD5KeyCreator = new StringMD5KeyCreator();
        System.out.printf("%s\t%s\n", args[0], stringMD5KeyCreator.createKey(args[0]));
        /*
        key = stringMD5KeyCreator.createKey("GeorgeWashington");
        numSubKeys = 5;
        subKeys = stringMD5KeyCreator.createSubKeys(key, numSubKeys);
        System.out.println(key);
        for (int i = 0; i < subKeys.length; i++) {
            System.out.printf("%d\t%s\n", i, subKeys[i]);
        }
        */
    }
}

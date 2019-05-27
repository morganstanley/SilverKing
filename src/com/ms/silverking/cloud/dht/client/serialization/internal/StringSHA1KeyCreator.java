package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.common.DHTKey;


public class StringSHA1KeyCreator extends BaseKeyCreator<String> {
    public StringSHA1KeyCreator() {
        super();
    }
    
    @Override
    public DHTKey createKey(String key) {
        return sha1KeyDigest.computeKey(key.getBytes());
    }
    
    public static void main(String[] args) {
        StringSHA1KeyCreator stringSHA1KeyCreator;
        DHTKey      key;
        DHTKey[]    subKeys;
        int         numSubKeys;

        stringSHA1KeyCreator = new StringSHA1KeyCreator();
        key = stringSHA1KeyCreator.createKey("GeorgeWashington");
        numSubKeys = 5;
        subKeys = stringSHA1KeyCreator.createSubKeys(key, numSubKeys);
        System.out.println(key);
        for (int i = 0; i < subKeys.length; i++) {
            System.out.printf("%d\t%s\n", i, subKeys[i]);
        }
    }
}

package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.numeric.NumConversion;


public class IntegerSHA1KeyCreator extends BaseKeyCreator<Integer> {
    public IntegerSHA1KeyCreator() {
        super();
    }
    
    @Override
    public DHTKey createKey(Integer key) {
        return md5KeyDigest.computeKey(NumConversion.intToBytes(key));
    }
    
    public static void main(String[] args) {
        IntegerSHA1KeyCreator integerSHA1KeyCreator;
        DHTKey      key;
        DHTKey[]    subKeys;
        int         numSubKeys;

        integerSHA1KeyCreator = new IntegerSHA1KeyCreator();
        key = integerSHA1KeyCreator.createKey(12345);
        numSubKeys = 5;
        subKeys = integerSHA1KeyCreator.createSubKeys(key, numSubKeys);
        System.out.println(key);
        for (int i = 0; i < subKeys.length; i++) {
            System.out.printf("%d\t%s\n", i, subKeys[i]);
        }
    }
}

package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.numeric.NumConversion;


public class IntegerMD5KeyCreator extends BaseKeyCreator<Integer> {
    public IntegerMD5KeyCreator() {
        super();
    }
    
    @Override
    public DHTKey createKey(Integer key) {
        return md5KeyDigest.computeKey(NumConversion.intToBytes(key));
    }
    
    public static void main(String[] args) {
        IntegerMD5KeyCreator integerMD5KeyCreator;
        DHTKey      key;
        DHTKey[]    subKeys;
        int         numSubKeys;

        integerMD5KeyCreator = new IntegerMD5KeyCreator();
        key = integerMD5KeyCreator.createKey(12345);
        numSubKeys = 5;
        subKeys = integerMD5KeyCreator.createSubKeys(key, numSubKeys);
        System.out.println(key);
        for (int i = 0; i < subKeys.length; i++) {
            System.out.printf("%d\t%s\n", i, subKeys[i]);
        }
    }
}

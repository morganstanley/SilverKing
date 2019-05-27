package com.ms.silverking.cloud.dht.crypto;

import java.security.MessageDigest;

import com.ms.silverking.cloud.dht.client.impl.KeyDigest;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.numeric.NumConversion;

public class MD5KeyDigest implements KeyDigest {
    public MD5KeyDigest() {
    }
    
    @Override
    public DHTKey computeKey(byte[] bytes) {
        MessageDigest   md;
        
        md = MD5Digest.getLocalMessageDigest();
        md.update(bytes, 0, bytes.length);
        return new SimpleKey(md.digest());
    }

    public DHTKey computeKey(String s) {
        MessageDigest   md;
        
        md = MD5Digest.getLocalMessageDigest();
        for (int i = 0; i < s.length(); i++) {
            md.update((byte)s.charAt(i));
        }
        return new SimpleKey(md.digest());
    }
    
    @Override
    public byte[] getSubKeyBytes(DHTKey key, int subKeyIndex) {
        byte[]  keyBytes;
        
        keyBytes = new byte[MD5Digest.BYTES];
        NumConversion.longToBytes(key.getMSL(), keyBytes, 0);
        NumConversion.longToBytes(key.getLSL() + subKeyIndex, keyBytes, NumConversion.BYTES_PER_LONG);
        return keyBytes;
    }
    
    @Override
    public DHTKey[] createSubKeys(DHTKey key, int numSubKeys) {
        DHTKey[]    subKeys;
        
        subKeys = new DHTKey[numSubKeys];
        for (int i = 0; i < subKeys.length; i++) {
            subKeys[i] = computeKey(getSubKeyBytes(key, i));
        }
        return subKeys;
    }
}

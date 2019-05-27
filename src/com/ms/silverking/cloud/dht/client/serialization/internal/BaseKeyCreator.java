package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.client.impl.KeyCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.crypto.MD5KeyDigest;
import com.ms.silverking.cloud.dht.crypto.SHA1KeyDigest;
import com.ms.silverking.numeric.NumConversion;


public abstract class BaseKeyCreator<K> implements KeyCreator<K> {
    protected final MD5KeyDigest  md5KeyDigest;
    protected final SHA1KeyDigest sha1KeyDigest;
    
    /*
     * Future - break apart the serialization portion and keydigest parts.
     */
    
    public BaseKeyCreator() {
        md5KeyDigest = new MD5KeyDigest();
        sha1KeyDigest = new SHA1KeyDigest();
    }
    
    private byte[] getSubKeyBytes(DHTKey key, int subKeyIndex) {
        byte[]  keyBytes;
        
        keyBytes = new byte[2 * NumConversion.BYTES_PER_LONG];
        NumConversion.longToBytes(key.getMSL(), keyBytes, 0);
        NumConversion.longToBytes(key.getLSL() + subKeyIndex, keyBytes, NumConversion.BYTES_PER_LONG);
        return keyBytes;
    }
    
    @Override
    public DHTKey[] createSubKeys(DHTKey key, int numSubKeys) {
        DHTKey[]    subKeys;
        
        subKeys = new DHTKey[numSubKeys];
        for (int i = 0; i < subKeys.length; i++) {
            subKeys[i] = md5KeyDigest.computeKey(getSubKeyBytes(key, i));
        }
        return subKeys;
    }
}

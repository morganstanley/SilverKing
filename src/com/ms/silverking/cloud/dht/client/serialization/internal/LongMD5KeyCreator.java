package com.ms.silverking.cloud.dht.client.serialization.internal;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.numeric.NumConversion;


public class LongMD5KeyCreator extends BaseKeyCreator<Long> {
    public LongMD5KeyCreator() {
        super();
    }
    
    @Override
    public DHTKey createKey(Long key) {
        return md5KeyDigest.computeKey(NumConversion.longToBytes(key));
    }
}

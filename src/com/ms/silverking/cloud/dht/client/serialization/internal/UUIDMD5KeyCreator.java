package com.ms.silverking.cloud.dht.client.serialization.internal;

import java.util.UUID;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.id.UUIDUtil;


public class UUIDMD5KeyCreator extends BaseKeyCreator<UUID> {
    public UUIDMD5KeyCreator() {
        super();
    }
    
    @Override
    public DHTKey createKey(UUID key) {
        return md5KeyDigest.computeKey(UUIDUtil.uuidToBytes(key));
    }
}

package com.ms.silverking.cloud.dht.net;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyedResult;
import com.ms.silverking.cloud.dht.common.OpResult;

public class PutResult extends KeyedResult {
    public PutResult(DHTKey key, OpResult result) {
        super(key, result);
    }
}

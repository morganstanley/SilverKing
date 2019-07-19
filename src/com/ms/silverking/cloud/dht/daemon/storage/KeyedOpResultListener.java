package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;

public interface KeyedOpResultListener {
    public void sendResult(DHTKey key, OpResult result);
}

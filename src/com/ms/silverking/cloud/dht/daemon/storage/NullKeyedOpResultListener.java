package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;

/**
 * Ignores results
 */
public class NullKeyedOpResultListener implements KeyedOpResultListener {
    public static final NullKeyedOpResultListener   instance = new NullKeyedOpResultListener();
    
    @Override
    public void sendResult(DHTKey key, OpResult result) {
    }
}

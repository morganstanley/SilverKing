package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.common.DHTKey;

public interface ActiveKeyedOperationResultListener<R> {
    void resultReceived(DHTKey key, R result);
}

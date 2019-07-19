package com.ms.silverking.cloud.dht.daemon;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.DHTKey;

public interface Waiter {
    public void waitForTriggered(DHTKey key, ByteBuffer result);
    public void relayWaitForResults();
}

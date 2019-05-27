package com.ms.silverking.cloud.dht.common;


// FUTURE - Consider whether this class is necessary
public interface OpResultListener {
    public void resultUpdated(DHTKey key, OpResult opResult);
}

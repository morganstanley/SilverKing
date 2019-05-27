package com.ms.silverking.cloud.dht.common;

public class KeyedResult {
    private final DHTKey key;
    private final OpResult result;

    public KeyedResult(DHTKey key, OpResult result) {
        this.key = key;
        this.result = result;
    }

    public DHTKey getKey() {
        return key;
    }

    public OpResult getResult() {
        return result;
    }

    @Override
    public String toString() {
        return key + ":" + result;
    }
}

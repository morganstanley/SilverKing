package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.common.DHTKey;

final class NoopReadThroughPlugin extends ReadThroughPlugin {

    @Override
    protected ByteBuffer[] readThroughBatchImpl(DHTKey[] keys, byte[] userOptions) {
        return new ByteBuffer[keys.length];
    }

    @Override
    protected ByteBuffer readThroughSingleImpl(DHTKey key, byte[] userOptions) {
        return null;
    }

    @Override
    String getName() {
        return this.getClass().getCanonicalName();
    }
}

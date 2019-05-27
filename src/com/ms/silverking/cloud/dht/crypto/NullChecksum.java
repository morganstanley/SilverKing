package com.ms.silverking.cloud.dht.crypto;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.client.impl.Checksum;

public class NullChecksum implements Checksum {
    @Override
    public void checksum(byte[] source, int position, int length, ByteBuffer dest) {
    }
    
    @Override
    public byte[] checksum(byte[] source, int position, int length) {
        return null;
    }

    @Override
    public void checksum(ByteBuffer source, ByteBuffer dest) {
    }

    @Override
    public byte[] checksum(ByteBuffer source) {
        return null;
    }

    @Override
    public void emptyChecksum(ByteBuffer dest) {
    }

    @Override
    public boolean isEmpty(byte[] actualChecksum) {
        return true;
    }

    @Override
    public boolean uniquelyIdentifiesValues() {
        return false;
    }
}

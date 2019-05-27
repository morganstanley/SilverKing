package com.ms.silverking.cloud.dht.client.impl;

import java.nio.ByteBuffer;

public interface Checksum {
    public void checksum(byte[] source, int position, int length, ByteBuffer dest);
    public byte[] checksum(byte[] source, int position, int length);
    public void checksum(ByteBuffer source, ByteBuffer dest);
    public byte[] checksum(ByteBuffer source);
    public void emptyChecksum(ByteBuffer dest);
    public boolean isEmpty(byte[] actualChecksum);
    public boolean uniquelyIdentifiesValues();
}

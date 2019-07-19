package com.ms.silverking.cloud.dht.client.serialization.internal;

import java.nio.ByteBuffer;

class Util {
    public static ByteBuffer[] wrapInByteBuffer(byte[] b) {
        ByteBuffer[]    bb;
        
        bb = new ByteBuffer[0];
        bb[0] = ByteBuffer.wrap(b);
        return bb;
    }
}

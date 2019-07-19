package com.ms.silverking.cloud.dht.net.protocol;

import java.nio.ByteBuffer;

import com.ms.silverking.numeric.NumConversion;



public class PutUpdateMessageFormat extends KeyedMessageFormat {
    public static final int versionSize = NumConversion.BYTES_PER_LONG;
    public static final int storageStateSize = 1;
    
    public static final int versionOffset = 0;
    public static final int storageStateOffset = versionSize;
    
    public static final int optionBytesSize = versionSize + storageStateSize;

    public static byte getStorageState(ByteBuffer buf) {
        return buf.get(storageStateOffset);
    }
}

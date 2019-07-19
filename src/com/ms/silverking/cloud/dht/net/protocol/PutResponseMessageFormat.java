package com.ms.silverking.cloud.dht.net.protocol;

import com.ms.silverking.numeric.NumConversion;

public class PutResponseMessageFormat extends KeyValueMessageFormat {
    // options buffer

    public static final int versionSize = NumConversion.BYTES_PER_LONG;
    public static final int storageStateSize = 1;
    
    public static final int versionOffset = 0;
    public static final int storageStateOffset = versionSize;
    
    public static final int optionBytesSize = versionSize + storageStateSize;
}

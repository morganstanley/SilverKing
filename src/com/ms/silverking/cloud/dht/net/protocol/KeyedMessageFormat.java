package com.ms.silverking.cloud.dht.net.protocol;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.numeric.NumConversion;

public class KeyedMessageFormat extends MessageFormat {
    public static final int    baseBytesPerKeyEntry = DHTKey.BYTES_PER_KEY; 
    public static final int    size = baseBytesPerKeyEntry;
    
    public static final int    keyMslOffset = 0;
    public static final int    keyLslOffset = NumConversion.BYTES_PER_LONG;
}

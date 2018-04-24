package com.ms.silverking.cloud.dht.net.protocol;

import com.ms.silverking.numeric.NumConversion;

public class KeyValueMessageFormat extends KeyedMessageFormat {
    public static final int    bufferIndexSize = NumConversion.BYTES_PER_INT;
    public static final int    bufferOffsetSize = NumConversion.BYTES_PER_INT;
    
    public static final int    bufferIndexOffset = KeyedMessageFormat.size;
    public static final int    bufferOffsetOffset = bufferIndexOffset + bufferIndexSize;
    
    public static final int    size = KeyedMessageFormat.size + bufferIndexSize + bufferOffsetSize;    
    
    // buffer format
    
    public static final int    optionBufferIndex = 1;    
}

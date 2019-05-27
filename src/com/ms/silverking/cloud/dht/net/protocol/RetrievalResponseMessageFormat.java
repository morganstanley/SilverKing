package com.ms.silverking.cloud.dht.net.protocol;

import com.ms.silverking.numeric.NumConversion;

public class RetrievalResponseMessageFormat extends KeyValueMessageFormat {
    // key buffer
    
    public static final int    resultLengthSize = NumConversion.BYTES_PER_INT;
    
    public static final int    resultLengthOffset = KeyValueMessageFormat.size;
    public static final int    size = resultLengthOffset + resultLengthSize;
    
    // options buffer

    public static final int retrievalTypeWaitModeSize = 1;
    public static final int internalOptionsSize = 1;
    public static final int vcMinSize = NumConversion.BYTES_PER_LONG;
    public static final int vcMaxSize = NumConversion.BYTES_PER_LONG;
    public static final int vcModeSize = 1;    
    public static final int vcMaxStorageTimeSize = NumConversion.BYTES_PER_LONG;
    
    public static final int retrievalTypeWaitModeOffset = 0;
    public static final int miscOptionsOffset = retrievalTypeWaitModeOffset + retrievalTypeWaitModeSize;
    public static final int vcMinOffset = miscOptionsOffset + internalOptionsSize;
    public static final int vcMaxOffset = vcMinOffset + vcMinSize;
    public static final int vcModeOffset = vcMaxOffset + vcMaxSize;    
    public static final int vcMaxStorageTimeOffset = vcModeOffset + vcModeSize;    
    
    public static final int optionBytesSize = retrievalTypeWaitModeSize + internalOptionsSize
                                        + vcMinSize + vcMaxSize + vcModeSize + vcMaxStorageTimeSize;
}

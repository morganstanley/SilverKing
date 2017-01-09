package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.time.SystemTimeSource;

public class SystemTimeUtil {
    public static final SystemTimeSource systemTimeSource 
                            = SystemTimeSource.createWithMillisOrigin(DHTConstants.nanoOriginTimeInMillis);    
}

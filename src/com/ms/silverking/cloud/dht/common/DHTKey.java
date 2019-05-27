package com.ms.silverking.cloud.dht.common;

import com.ms.silverking.numeric.NumConversion;

/**
 * Internal DHT key representation derived from a hash of the user key 
 */
public interface DHTKey {
    public static int   BYTES_PER_KEY = 2 * NumConversion.BYTES_PER_LONG;
    
    public long getMSL();
    public long getLSL();
}

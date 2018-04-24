package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.impl.SystemChecksum;

/**
 * Checksum to use for DHT values.
 */
public enum ChecksumType {
    NONE, MD5, SHA_1, MURMUR3_32, MURMUR3_128, SYSTEM;
	
    public int length() {
        switch (this) {
        case NONE: return 0;
        case MD5: return 16;
        case SHA_1: return 20;
        case MURMUR3_32: return 4;
        case MURMUR3_128: return 16;
        case SYSTEM: return SystemChecksum.BYTES;
        default: throw new RuntimeException("panic");
        }
    }
}

package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.crypto.HashFunctionChecksum;
import com.ms.silverking.cloud.dht.crypto.MD5Checksum;
import com.ms.silverking.cloud.dht.crypto.NullChecksum;

public class ChecksumProvider {
    private static final Checksum   md5Checksum = new MD5Checksum();
    private static final Checksum   nullChecksum = new NullChecksum();
    private static final Checksum   murmur3_32Checksum = new HashFunctionChecksum(HashFunctionChecksum.Type.Murmur3_32);
    private static final Checksum   murmur3_128Checksum = new HashFunctionChecksum(HashFunctionChecksum.Type.Murmur3_128);
    private static final Checksum   systemChecksum = new SystemChecksum(); 
    
    public static Checksum getChecksum(ChecksumType checksumType) {
        switch (checksumType) {
        case MD5: return md5Checksum;
        case NONE: return nullChecksum;
        case MURMUR3_32: return murmur3_32Checksum;
        case MURMUR3_128: return murmur3_128Checksum;
        case SYSTEM: return systemChecksum;
        default: throw new RuntimeException("No provider for: "+ checksumType);
        }
    }
}

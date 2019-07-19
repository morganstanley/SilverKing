package com.ms.silverking.cloud.dht.crypto;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Arrays;

import com.ms.silverking.cloud.dht.client.impl.Checksum;

public class SHA1Checksum implements Checksum {
    private static final int    BYTES = 20;
    
    private static final byte[] emptyChecksum = new byte[BYTES];
    
    @Override
    public void checksum(byte[] source, int position, int length, ByteBuffer dest) {
        dest.put(checksum(source, position, length));
    }
    
    @Override
    public byte[] checksum(byte[] source, int position, int length) {
            MessageDigest   md;
            
            md = MD5Digest.getLocalMessageDigest();
            md.update(source, position, length);
            return md.digest();
    }

    @Override
    public void checksum(ByteBuffer source, ByteBuffer dest) {
        dest.put(checksum(source));
    }

    @Override
    public byte[] checksum(ByteBuffer source) {
        MessageDigest   md;
        
        md = SHA1Digest.getLocalMessageDigest();
        md.update(source);
        return md.digest();
    }

    @Override
    public void emptyChecksum(ByteBuffer dest) {
        dest.put(emptyChecksum);
    }

    @Override
    public boolean isEmpty(byte[] checksum) {
        return Arrays.equals(checksum, emptyChecksum);
    }

    @Override
    public boolean uniquelyIdentifiesValues() {
        return true;
    }
}

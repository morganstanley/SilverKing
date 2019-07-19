package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import com.ms.silverking.cloud.dht.crypto.MD5Digest;
import com.ms.silverking.numeric.NumConversion;

/**
 * A RingID is a hash of a ring name to enable short, fixed-length identification of a ring. 
 */
public class RingID {
    private final long  msb;
    private final long  lsb;
    
    public static final int BYTES = MD5Digest.BYTES;
    
    public RingID(long msb, long lsb) {
        this.msb = msb;
        this.lsb = lsb;
    }
    
    public static RingID nameToRingID(String ringName) {
        MessageDigest   digest;
        byte[]          hash;
        long            msb;
        long            lsb;
        
        digest = MD5Digest.getLocalMessageDigest();
        digest.update(ringName.getBytes());
        hash = digest.digest();
        assert hash.length == MD5Digest.BYTES;
        msb = NumConversion.bytesToLong(hash, 0);
        lsb = NumConversion.bytesToLong(hash, NumConversion.BYTES_PER_LONG);
        return new RingID(msb, lsb);
    }
    
    public void writeToBuffer(ByteBuffer buf) {
        buf.putLong(msb);
        buf.putLong(lsb);
    }

    public static RingID readFromBuffer(ByteBuffer buf, int ringIDOffset) {
        long    msb;
        long    lsb;
        
        msb = buf.getLong(ringIDOffset);
        lsb = buf.getLong(ringIDOffset + NumConversion.BYTES_PER_LONG);
        return new RingID(msb, lsb);
    }
    
    @Override
    public boolean equals(Object other) {
        RingID  oRingID;
        
        oRingID = (RingID)other;
        return msb == oRingID.msb && lsb == oRingID.lsb;
    }
    
    @Override
    public int hashCode() {
        return (int)(msb >>> 32) ^ (int)lsb;
    }
    
    @Override
    public String toString() {
        return String.format("%x:%x", msb, lsb);
    }
}

package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Arrays;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.text.StringUtil;

/**
 * Key and value checksum pair for use in convergence.
 */
public class KeyValueChecksum {
    private final DHTKey    key;
    private final byte[]    valueChecksum;
    
    public KeyValueChecksum(DHTKey key, byte[] valueChecksum) {
        this.key = new SimpleKey(key);
        this.valueChecksum = Arrays.copyOf(valueChecksum, valueChecksum.length);
    }
    
    public DHTKey getKey() {
        return key;
    }

    public byte[] getValueChecksum() {
        return valueChecksum;
    }
    
    @Override 
    public int hashCode() {
        return key.hashCode() ^ NumConversion.bytesToInt(valueChecksum, valueChecksum.length - NumConversion.BYTES_PER_INT);
    }
    
    @Override
    public boolean equals(Object other) {
        KeyValueChecksum    oKVC;
        
        oKVC = (KeyValueChecksum)other;
        return key.equals(oKVC.key) && Arrays.equals(valueChecksum, oKVC.valueChecksum);
    }

    @Override
    public String toString() {
        return key.toString() +" "+ StringUtil.byteArrayToHexString(valueChecksum); 
    }
}

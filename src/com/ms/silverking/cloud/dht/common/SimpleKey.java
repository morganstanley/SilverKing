package com.ms.silverking.cloud.dht.common;

import java.util.concurrent.ThreadLocalRandom;

import com.ms.silverking.numeric.NumConversion;

public class SimpleKey implements DHTKey, Comparable<DHTKey> {
    private final long  msl;
    private final long  lsl;
    
    public SimpleKey(long msl, long lsl) {
        this.msl = msl;
        this.lsl = lsl;
    }

    public SimpleKey(byte[] bytes) {
        this(NumConversion.bytesToLong(bytes, 0), 
              NumConversion.bytesToLong(bytes, NumConversion.BYTES_PER_LONG));
    }
    
    public SimpleKey(DHTKey key) {
        this.msl = key.getMSL();
        this.lsl = key.getLSL();
    }

    public static SimpleKey mapToSimpleKey(byte[] bytes) {
        assert bytes.length < NumConversion.BYTES_PER_LONG * 2;
        return new SimpleKey(NumConversion.bytesToLong(bytes, 0), 
              NumConversion.bytesToLong(bytes, NumConversion.BYTES_PER_LONG));
    }
    
    public static SimpleKey randomKey() {
        return new SimpleKey(ThreadLocalRandom.current().nextLong(), ThreadLocalRandom.current().nextLong()); 
    }
    
    @Override
    public long getMSL() {
        return msl;
    }

    @Override
    public long getLSL() {
        return lsl;
    }

    @Override
    public int hashCode() {
        // this presumes that this key is strongly random
        // works fine for crypto-hash-derived keys
        return (int)lsl;
    }
    
    @Override
    public boolean equals(Object o) {
        DHTKey   oKey;
        
        oKey = (DHTKey)o;
        return lsl == oKey.getLSL() && msl == oKey.getMSL();
    }
    
    @Override
    public String toString() {
        return KeyUtil.keyToString(this);
    }

    @Override
    public int compareTo(DHTKey o) {
        if (msl < o.getMSL()) {
            return -1;
        } else if (msl > o.getMSL()) {
            return 1;
        } else {
            if (lsl < o.getLSL()) {
                return -1;
            } else if (lsl > o.getLSL()) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}

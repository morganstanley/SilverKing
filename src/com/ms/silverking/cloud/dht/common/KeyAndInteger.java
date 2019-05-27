package com.ms.silverking.cloud.dht.common;

import java.util.Comparator;

public class KeyAndInteger implements DHTKey {
    private final long  msl;
    private final long  lsl;
    private final int   integer;
    
    public KeyAndInteger(long msl, long lsl, int integer) {
        this.msl = msl;
        this.lsl = lsl;
        this.integer = integer;
    }

    public KeyAndInteger(DHTKey key, int integer) {
        this.msl = key.getMSL();
        this.lsl = key.getLSL();
        this.integer = integer;
    }

    @Override
    public long getMSL() {
        return msl;
    }

    @Override
    public long getLSL() {
        return lsl;
    }
    
    public int getInteger() {
        return integer;
    }

    @Override
    public int hashCode() {
        // this presumes that this key is strongly random
        // works fine for crypto-hash-derived keys
        return (int)lsl ^ integer;
    }
    
    @Override
    public boolean equals(Object o) {
        KeyAndInteger   oKey;
        
        oKey = (KeyAndInteger)o;
        return lsl == oKey.getLSL() && msl == oKey.getMSL() && integer == oKey.integer;
    }
    
    @Override
    public String toString() {
        return KeyUtil.keyToString(this) +":"+ integer;
    }
    
    public static Comparator<KeyAndInteger> getIntegerComparator() {
        return integerComparator;
    }
    
    private static final Comparator<KeyAndInteger>  integerComparator = new IntegerComparator();
    
    private static class IntegerComparator implements Comparator<KeyAndInteger> {
        @Override
        public int compare(KeyAndInteger k1, KeyAndInteger k2) {
            if (k1.integer < k2.integer) {
                return -1;
            } else if (k1.integer > k2.integer) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}

package com.ms.silverking.cloud.dht.common;

import java.util.Comparator;

public class UnsignedKeyComparator implements Comparator<DHTKey> {
    public static final UnsignedKeyComparator instance = new UnsignedKeyComparator();
    
    public UnsignedKeyComparator() {
    }

    @Override
    public int compare(DHTKey k1, DHTKey k2) {
        return KeyUtil.keyToBigInteger(k1).compareTo(KeyUtil.keyToBigInteger(k2));
    }
}

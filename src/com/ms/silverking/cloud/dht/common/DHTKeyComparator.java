package com.ms.silverking.cloud.dht.common;

import java.util.Comparator;

public class DHTKeyComparator implements Comparator<DHTKey >{
    public static DHTKeyComparator  dhtKeyComparator = new DHTKeyComparator();
    
    public DHTKeyComparator() {
    }
    
    @Override
    public int compare(DHTKey k1, DHTKey k2) {
        if (k1.getMSL() < k2.getMSL()) {
            return -1;
        } else if (k1.getMSL() > k2.getMSL()) {
            return 1;
        } else {
            if (k1.getLSL() < k2.getLSL()) {
                return -1;
            } else if (k1.getLSL() > k2.getLSL()) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}

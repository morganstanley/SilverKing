package com.ms.silverking.net.analysis.iperf;

import java.util.Comparator;

public class BPSComparator implements Comparator<Measurement> {
    public BPSComparator() {
    }
    @Override
    public int compare(Measurement o1, Measurement o2) {
        long    bps1;
        long    bps2;
        
        bps1 = o1.getBitsPerSecond();
        bps2 = o2.getBitsPerSecond();
        if (bps1 < bps2) {
            return -1;
        } else if (bps1 > bps2) {
            return 1;
        } else {
            return 0;
        }
    }
}

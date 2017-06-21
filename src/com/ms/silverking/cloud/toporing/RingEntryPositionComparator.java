package com.ms.silverking.cloud.toporing;

import java.util.Comparator;

import com.ms.silverking.cloud.ring.RingRegion;

public class RingEntryPositionComparator implements Comparator<RingEntry> {
	public static final RingEntryPositionComparator	instance = new RingEntryPositionComparator();
	
    public RingEntryPositionComparator() {
    }
    
    @Override
    public int compare(RingEntry o1, RingEntry o2) {
        return RingRegion.positionComparator.compare(o1.getRegion(), o2.getRegion()); 
    }
}

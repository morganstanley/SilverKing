package com.ms.silverking.cloud.ring;

import java.util.Comparator;

public class RingRegionSizePositionComparator implements Comparator<RingRegion> {
	@Override
	public int compare(RingRegion r0, RingRegion r1) {		
		if (r0.getSize() > r1.getSize()) {
			return 1;
		} else if (r0.getSize() < r1.getSize()) {
			return -1;
		} else {
			if (r0.getStart() > r1.getStart()) {
				return 1;
			} else if (r0.getStart() < r1.getStart()) {
				return -1;
			} else {
				return 0;
			}
		}
	}
}

package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Set;

import com.ms.silverking.log.Log;

public class RetainedOffsetMapCheck implements EntryRetentionCheck {
	private final Set<Integer>	retainedOffsets;
	private final Set<Integer>	discardedOffsets;
	
	public RetainedOffsetMapCheck(Set<Integer> retainedOffsets, Set<Integer> discardedOffsets) {
		this.retainedOffsets = retainedOffsets;
		this.discardedOffsets = discardedOffsets;
	}

	@Override
	public boolean shouldRetain(int segmentNumber, DataSegmentWalkEntry entry) {
		int	offset;
		
		offset = entry.getOffset();
		if (retainedOffsets.contains(offset)) {
			return true;
		} else if (discardedOffsets.contains(offset)) {
			return false;
		} else {
			Log.warningf("Unexpected unknown offset in RetainedOffsetMapCheck.shouldRetain() %s %d", entry.getKey(), offset);
			return true;
		}
	}
}

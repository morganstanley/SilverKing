package com.ms.silverking.cloud.dht.daemon.storage;

public interface EntryRetentionCheck {
	public boolean shouldRetain(int segmentNumber, DataSegmentWalkEntry entry);
}

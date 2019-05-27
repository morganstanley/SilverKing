package com.ms.silverking.cloud.dht.daemon.storage;


class TestRetentionCheck implements EntryRetentionCheck {
	private final int	retentionLimit;
	
	TestRetentionCheck(int retentionLimit) {
		this.retentionLimit = retentionLimit;
	}

	@Override
	public boolean shouldRetain(int segmentNumber, DataSegmentWalkEntry entry) {
		return entry.getOffset() <= retentionLimit;
	}
}

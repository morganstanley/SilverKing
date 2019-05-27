package com.ms.silverking.cloud.dht.daemon.storage;

class SegmentCompactionResult {
	private final boolean	isEmpty;
	private final double	compactionRatio;
	
	public SegmentCompactionResult(boolean isEmpty, double compactionRatio) {
		this.isEmpty = isEmpty;
		this.compactionRatio = compactionRatio;
	}
	
	public boolean isEmpty() {
		return isEmpty;
	}

	public double getCompactionRatio() {
		return compactionRatio;
	}
}

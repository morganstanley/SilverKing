package com.ms.silverking.cloud.dht.daemon.storage;

enum BufferMode {
	PreReadAll, PreReadIndex, InPlace;

	public boolean preReadDataSegment() {
		return this == PreReadAll;
	}
	
	public boolean preReadIndex() {
		return this != InPlace;
	}
}
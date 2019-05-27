package com.ms.silverking.cloud.dht.daemon.storage;

public class InvalidOffsetListIndexException extends RuntimeException {
	public InvalidOffsetListIndexException(int index) {
		super(Integer.toString(index));
	}
}

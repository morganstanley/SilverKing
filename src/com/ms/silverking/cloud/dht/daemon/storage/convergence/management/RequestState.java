package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

public enum RequestState {
	INCOMPLETE, SUCCEEDED, FAILED;
	
	public boolean isComplete() {
		return this != INCOMPLETE;
	}
}

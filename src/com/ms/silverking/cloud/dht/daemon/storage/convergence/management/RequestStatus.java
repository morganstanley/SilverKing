package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

public interface RequestStatus {
	public RequestState getRequestState();
	public boolean requestComplete();
	public String getStatusString();
}

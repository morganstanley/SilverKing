package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.Serializable;

public class SimpleRequestStatus implements RequestStatus, Serializable {
	private final RequestState	requestState;
	private final String		statusString;
	
	private static final long serialVersionUID = -9154548957915879684L;
	
	public SimpleRequestStatus(RequestState requestState, String statusString) {
		this.requestState = requestState;
		this.statusString = statusString;
	}
	
	@Override
	public RequestState getRequestState() {
		return requestState;
	}
	
	@Override
	public boolean requestComplete() {
		return requestState.isComplete();
	}

	@Override
	public String getStatusString() {
		return statusString;
	}
	
	@Override
	public int hashCode() {
		return requestState.hashCode() ^ statusString.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		SimpleRequestStatus	o;
		
		o = (SimpleRequestStatus)obj;
		return this.requestState == o.requestState && this.statusString.equals(o.statusString);
	}
}

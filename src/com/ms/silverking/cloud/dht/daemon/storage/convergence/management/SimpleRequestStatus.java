package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.Serializable;

public class SimpleRequestStatus implements RequestStatus, Serializable {
	private final boolean	isComplete;
	private final String	statusString;
	
	private static final long serialVersionUID = -2302840723977626560L;
	
	public SimpleRequestStatus(boolean isComplete, String statusString) {
		this.isComplete = isComplete;
		this.statusString = statusString;
	}
	
	@Override
	public boolean requestComplete() {
		return isComplete;
	}

	@Override
	public String getStatusString() {
		return statusString;
	}
	
	@Override
	public int hashCode() {
		return Boolean.hashCode(isComplete) ^ statusString.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		SimpleRequestStatus	o;
		
		o = (SimpleRequestStatus)obj;
		return this.isComplete == o.isComplete && this.statusString.equals(o.statusString);
	}
}

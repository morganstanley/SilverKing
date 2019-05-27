package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import com.ms.silverking.id.UUIDBase;

public interface RequestController {
	public void stop(UUIDBase uuid);
	public void waitForCompletion(UUIDBase uuid);
	public RequestStatus getStatus(UUIDBase uuid);
}

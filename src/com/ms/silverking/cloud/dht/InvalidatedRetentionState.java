package com.ms.silverking.cloud.dht;

import java.util.HashSet;
import java.util.Set;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;

public class InvalidatedRetentionState implements ValueRetentionState {
	private final Set<DHTKey>	invalidatedKeys;
	
	@OmitGeneration
	public InvalidatedRetentionState() {
		invalidatedKeys = new HashSet<>();
	}

	public boolean isInvalidated(DHTKey key) {
		return invalidatedKeys.contains(key);
	}
	
	public void setInvalidated(DHTKey key) {
		invalidatedKeys.add(key);
	}
}

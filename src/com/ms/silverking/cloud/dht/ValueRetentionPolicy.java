package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.common.DHTKey;


public interface ValueRetentionPolicy<T extends ValueRetentionState> {
	public enum ImplementationType {SingleReverseSegmentWalk,RetainAll};
	
	public ImplementationType getImplementationType();	
	public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated, 
						   T valueRetentionState, long curTimeNanos);
	public T createInitialState();	
}

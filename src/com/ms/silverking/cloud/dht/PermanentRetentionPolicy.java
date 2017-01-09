package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.text.ObjectDefParser2;

public class PermanentRetentionPolicy implements ValueRetentionPolicy<ValueRetentionState> {
	private static final PermanentRetentionPolicy	template = new PermanentRetentionPolicy();

	static {
        ObjectDefParser2.addParser(template);
    }
	
	public PermanentRetentionPolicy() {
	}
	
	@Override
	public ImplementationType getImplementationType() {
		return ImplementationType.RetainAll;
	}

	@Override
	public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated, 
						   ValueRetentionState state, long curTimeNanos) {
		return true;
	}

	@Override
	public ValueRetentionState createInitialState() {
		return null;
	}
	
	@Override
	public int hashCode() {
		return 0;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof PermanentRetentionPolicy) {
			return true;
		} else {
			throw new ClassCastException();
		}
	}
	
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
	
    public static PermanentRetentionPolicy parse(String def) {
        return ObjectDefParser2.parse(PermanentRetentionPolicy.class, def);
    }	
}

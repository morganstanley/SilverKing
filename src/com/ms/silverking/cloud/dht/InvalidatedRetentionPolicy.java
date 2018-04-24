package com.ms.silverking.cloud.dht;

import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.time.SystemTimeSource;

public class InvalidatedRetentionPolicy implements ValueRetentionPolicy<InvalidatedRetentionState> {
	private final long	invalidatedRetentionIntervalSeconds;
	
	static final InvalidatedRetentionPolicy	template = new InvalidatedRetentionPolicy(0);

	static {
        ObjectDefParser2.addParser(template);
    }
	
	@OmitGeneration
	public InvalidatedRetentionPolicy(long invalidatedRetentionIntervalSeconds) {
		this.invalidatedRetentionIntervalSeconds = invalidatedRetentionIntervalSeconds;
	}
	
	@Override
	public ImplementationType getImplementationType() {
		return ImplementationType.SingleReverseSegmentWalk;
	}

	@Override
	public boolean retains(DHTKey key, long version, long creationTimeNanos, boolean invalidated, 
						   InvalidatedRetentionState invalidatedRetentionState, long curTimeNanos) {
		if (!invalidated) {
			if (invalidatedRetentionState.isInvalidated(key)) {
				invalidated = true;
			}
		} else {
			invalidatedRetentionState.setInvalidated(key);
		}
		if (invalidated) {
			long	invalidatedRetentionIntervalNanos;
			
			invalidatedRetentionIntervalNanos = TimeUnit.NANOSECONDS.convert(invalidatedRetentionIntervalSeconds, TimeUnit.SECONDS);
			return creationTimeNanos + invalidatedRetentionIntervalNanos > curTimeNanos;
		} else {
			return true;
		}
	}

	@Override
	public InvalidatedRetentionState createInitialState() {
		return new InvalidatedRetentionState();
	}
	
	@Override
	public int hashCode() {
		return Long.hashCode(invalidatedRetentionIntervalSeconds);
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		
		if (this.getClass() != o.getClass()) {
			return false;
		}
		
		InvalidatedRetentionPolicy	other;
		
		other = (InvalidatedRetentionPolicy)o;
		return invalidatedRetentionIntervalSeconds == other.invalidatedRetentionIntervalSeconds;
	}
	
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
	
    public static InvalidatedRetentionPolicy parse(String def) {
        return ObjectDefParser2.parse(InvalidatedRetentionPolicy.class, def);
    }	
    
    public static void main(String[] args) {
    	String	def;
    	InvalidatedRetentionPolicy	irp;
    	InvalidatedRetentionPolicy	irp2;
    	
    	irp = new InvalidatedRetentionPolicy(10);
    	def = irp.toString();
    	System.out.println(def);
    	irp2 = parse(def);
    	System.out.println(irp2);
    	irp2 = parse("invalidatedRetentionIntervalSeconds=10");
    	System.out.println(irp2);
    	
    	InvalidatedRetentionState	invalidatedRetentionState;
    	long	creationTimeNanos;
    	long	curTimeNanos;
    	
    	invalidatedRetentionState = new InvalidatedRetentionState();
    	curTimeNanos = SystemTimeSource.instance.absTimeNanos();
    	creationTimeNanos = curTimeNanos;
    	System.out.println(irp.retains(new SimpleKey(0, 1), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos));
    	System.out.println(irp.retains(new SimpleKey(0, 2), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos));
    	creationTimeNanos = curTimeNanos - 100 * 1000000000L;
    	System.out.println(irp.retains(new SimpleKey(0, 3), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos));    	
    	System.out.println(irp.retains(new SimpleKey(0, 4), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos));
    	
    	System.out.println();
    	creationTimeNanos = curTimeNanos - 200 * 1000000000L;
    	System.out.println(irp.retains(new SimpleKey(0, 1), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos));
    	System.out.println(irp.retains(new SimpleKey(0, 2), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos));
    	System.out.println(irp.retains(new SimpleKey(0, 3), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos));    	
    	System.out.println(irp.retains(new SimpleKey(0, 4), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos));    	
    }
}

package com.ms.silverking.cloud.dht;

import java.util.concurrent.TimeUnit;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.time.SystemTimeSource;

public class NanosVersionRetentionPolicy implements ValueRetentionPolicy<InvalidatedRetentionState> {
	private final long	invalidatedRetentionIntervalSeconds;
	private final long	maxRetentionIntervalSeconds;
	
	public static final long	NO_MAX_RETENTION_INTERVAL = 0;
	
	static final NanosVersionRetentionPolicy	template = new NanosVersionRetentionPolicy(0, 0);

	static {
        ObjectDefParser2.addParser(template);
    }
	
	@OmitGeneration
	public NanosVersionRetentionPolicy(long invalidatedRetentionIntervalSeconds, long maxRetentionIntervalSeconds) {
		this.invalidatedRetentionIntervalSeconds = invalidatedRetentionIntervalSeconds;
		this.maxRetentionIntervalSeconds = maxRetentionIntervalSeconds;
	}
	
	@OmitGeneration
	public NanosVersionRetentionPolicy(long invalidatedRetentionIntervalSeconds) {
		this(invalidatedRetentionIntervalSeconds, NO_MAX_RETENTION_INTERVAL);
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
			if (maxRetentionIntervalSeconds == NO_MAX_RETENTION_INTERVAL) {
				return true;
			} else {
				long	maxRetentionIntervalNanos;
				boolean	retain;
				
				maxRetentionIntervalNanos = TimeUnit.NANOSECONDS.convert(maxRetentionIntervalSeconds, TimeUnit.SECONDS);
				retain = version + maxRetentionIntervalNanos > curTimeNanos;
				//System.out.printf("%s\t%s\t%d\t%d\t%d\t%d\n", key, retain, version, maxRetentionIntervalNanos, version + maxRetentionIntervalNanos, curTimeNanos);
				// for now, leave this out to make this a local segment notion
				//if (!retain) {
				//	invalidatedRetentionState.setInvalidated(key);
				//}
				return retain;
			}
		}
	}

	@Override
	public InvalidatedRetentionState createInitialState() {
		return new InvalidatedRetentionState();
	}
	
	@Override
	public int hashCode() {
		return Long.hashCode(invalidatedRetentionIntervalSeconds)
				^ Long.hashCode(maxRetentionIntervalSeconds);
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		
		if (this.getClass() != o.getClass()) {
			return false;
		}
		
		NanosVersionRetentionPolicy	other;
		
		other = (NanosVersionRetentionPolicy)o;
		return invalidatedRetentionIntervalSeconds == other.invalidatedRetentionIntervalSeconds
				&& maxRetentionIntervalSeconds == other.maxRetentionIntervalSeconds;
	}
	
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
	
    public static NanosVersionRetentionPolicy parse(String def) {
        return ObjectDefParser2.parse(NanosVersionRetentionPolicy.class, def);
    }	
    
    public static void main(String[] args) {
    	String	def;
    	NanosVersionRetentionPolicy	irp;
    	NanosVersionRetentionPolicy	irp2;
    	
    	irp = new NanosVersionRetentionPolicy(10, NO_MAX_RETENTION_INTERVAL);
    	def = irp.toString();
    	System.out.println(def);
    	irp2 = parse(def);
    	System.out.println(irp2);
    	irp2 = parse("invalidatedRetentionIntervalSeconds=10,maxRetentionIntervalSeconds=7");
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
    	
    	System.out.println();
    	System.out.println();
    	irp = new NanosVersionRetentionPolicy(10, TimeUnit.SECONDS.convert(7, TimeUnit.DAYS));
    	System.out.printf("7 day retention %s\n", irp);
    	creationTimeNanos = curTimeNanos;
    	System.out.println(irp.retains(new SimpleKey(0, 1), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos));
    	System.out.println(irp.retains(new SimpleKey(0, 2), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos));
    	creationTimeNanos = curTimeNanos - 100 * 1000000000L;
    	System.out.println(irp.retains(new SimpleKey(0, 3), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos));    	
    	System.out.println(irp.retains(new SimpleKey(0, 4), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos));
    	
    	System.out.println();
    	creationTimeNanos = curTimeNanos - TimeUnit.NANOSECONDS.convert(8, TimeUnit.DAYS);
    	System.out.println(irp.retains(new SimpleKey(0, 1), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos));
    	System.out.println(irp.retains(new SimpleKey(0, 2), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos));
    	System.out.println(irp.retains(new SimpleKey(0, 3), 0, creationTimeNanos, false, invalidatedRetentionState, curTimeNanos));    	
    	System.out.println(irp.retains(new SimpleKey(0, 4), 0, creationTimeNanos, true, invalidatedRetentionState, curTimeNanos));
    }
}

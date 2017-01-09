package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.text.ObjectDefParser2;

public class TimeRetentionPolicy implements ValueRetentionPolicy {
	private final Mode	mode;
	private final int	minVersions;
	private final long	timeSpanSeconds;
	
	// FIXME - this class has not been updated to use the single pass reverse walk implementation
	
	public enum Mode {wallClock, mostRecentValue};
	
	private static final TimeRetentionPolicy	template = new TimeRetentionPolicy(Mode.wallClock, 1, 86400);
	
    static {
        ObjectDefParser2.addParser(template);
    }    	
	
	public TimeRetentionPolicy(Mode mode, int minVersions, long timeSpanSeconds) {
		this.mode = mode;
		this.minVersions = minVersions;
		this.timeSpanSeconds = timeSpanSeconds;
	}
	
	public Mode getMode() {
		return mode;
	}

	public int getMinVersions() {
		return minVersions;
	}

	public long getTimeSpanSeconds() {
		return timeSpanSeconds;
	}
	
	public long getTimeSpanNanos() {
		return timeSpanSeconds * 1000000000;
	}
	
	public long getTimeSpanMillis() {
		return timeSpanSeconds * 1000;
	}
	
    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
    
    @Override
    public int hashCode() {
     	return mode.hashCode() ^ Integer.hashCode(minVersions) ^ Long.hashCode(timeSpanSeconds);
    }
    
    @Override
    public boolean equals(Object obj) {
    	TimeRetentionPolicy	o;
    	
    	o = (TimeRetentionPolicy)obj;
    	return mode == o.mode && minVersions == o.minVersions && timeSpanSeconds == o.timeSpanSeconds;
    }
    
    /**
     * Parse a definition 
     * @param def object definition 
     * @return a parsed instance 
     */
    public static TimeRetentionPolicy parse(String def) {
        return ObjectDefParser2.parse(TimeRetentionPolicy.class, def);
    }

	@Override
	public ImplementationType getImplementationType() {
		throw new RuntimeException("deco");
	}

	@Override
	public boolean retains(DHTKey key, long version, long creationTimeNanos,
			boolean invalidated, ValueRetentionState valueRetentionState,
			long curTimeNanos) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ValueRetentionState createInitialState() {
		// TODO Auto-generated method stub
		return null;
	}	
}

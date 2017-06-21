package com.ms.silverking.time;

/**
 * AbsMillisTimeSource returning a constant specified value.
 */
public final class ConstantAbsMillisTimeSource implements AbsMillisTimeSource {
    private final long  absMillisTime;
    
	public ConstantAbsMillisTimeSource(long absMillisTime) {
	    this.absMillisTime = absMillisTime;
	}

	@Override
	public long absTimeMillis() {
		return absMillisTime;
	}

    @Override
    public int relMillisRemaining(long absDeadlineMillis) {
        return TimeSourceUtil.relTimeRemainingAsInt(absDeadlineMillis, absTimeMillis());
    }
    
    @Override
    public int hashCode() {
    	return Long.hashCode(absMillisTime);
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    		
    	if (this.getClass() != o.getClass()) {
    		return false;
    	}
    	
    	ConstantAbsMillisTimeSource other = (ConstantAbsMillisTimeSource)o;
    	return absMillisTime == other.absMillisTime;
    }
}

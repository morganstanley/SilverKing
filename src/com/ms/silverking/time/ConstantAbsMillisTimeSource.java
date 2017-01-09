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
}

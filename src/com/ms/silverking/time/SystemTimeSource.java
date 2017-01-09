package com.ms.silverking.time;

/**
 * RelNanosAbsMillisTimeSource implemented with standard System class calls.
 *
 */
public final class SystemTimeSource implements RelNanosAbsMillisTimeSource, AbsNanosTimeSource {
	private final SafeAbsNanosTimeSource	absNanosTimeSource;
    
    public static final SystemTimeSource  instance = new SystemTimeSource();
    
	private SystemTimeSource(long nanoOriginInMillis) {
		absNanosTimeSource = new SafeAbsNanosTimeSource(nanoOriginInMillis);
	}

    public SystemTimeSource() {
        this(System.currentTimeMillis());
    }
    
    public static SystemTimeSource createWithMillisOrigin(long nanoOriginInMillis) {
        return new SystemTimeSource(nanoOriginInMillis);
    }
    
	@Override
	public long relTimeNanos() {
		return System.nanoTime();
	}

	@Override
	public long absTimeMillis() {
		return System.currentTimeMillis();
	}
	
    @Override
    public int relMillisRemaining(long absDeadlineMillis) {
        return TimeSourceUtil.relTimeRemainingAsInt(absDeadlineMillis, absTimeMillis());
    }
    
    @Override
    public long absTimeNanos() {
        return absNanosTimeSource.absTimeNanos();
    }
    
    @Override
    public long getNanosOriginTime() {
        return absNanosTimeSource.getNanosOriginTime();
    }

    @Override
    public long relNanosRemaining(long absDeadlineNanos) {
    	return absNanosTimeSource.relNanosRemaining(absDeadlineNanos);
    }
}

package com.ms.silverking.time;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;


/**
 * SafeAbsNanosTimeSource returns a "safe" absolute time in nano seconds by
 * filtering out observed issues in System.nanoTime(). In particular, this
 * class guards against both System.nanoTime() forward and backward excursions.
 * System.currentTimeMillis() provides the ground truth;. 
 * 
 * NOTE: Presently, the safety features are turned off as they need additional
 * testing before introducing them
 */
public class SafeAbsNanosTimeSource implements AbsNanosTimeSource {
    public final long		nanoOriginTimeInMillis;
    public final AtomicLong	nanoOriginTimeInNanos;
    public final AtomicLong	lastReturnedAbsTimeNanos;
    public final AtomicLong	lastSystemNanos;
    public final long		sanityCheckThresholdNanos;
    public final long		originDeltaToleranceNanos;
    
    private final AtomicLong	lastTimeNanos = new AtomicLong();
        
    private static final boolean	debug = false;
    
    private static final long   nanosPerMilli = 1000000;
    
    private static final long	defaultSanityCheckThresholdNanos;
	private static final String defaultSanityCheckThresholdNanosProperty = 
			SafeAbsNanosTimeSource.class.getName() + ".DefaultSanityCheckThresholdNanos";
    private static final long	defaultDefaultSanityCheckThresholdNanos = 20 * nanosPerMilli;
    
    private static final long	defaultOriginDeltaToleranceNanos;
	private static final String defaultOriginDeltaToleranceNanosProperty = 
			SafeAbsNanosTimeSource.class.getName() + ".DefaultOriginDeltaToleranceNanos";
    private static final long	defaultDefaultOriginDeltaToleranceNanos = 100 * nanosPerMilli;
    
    static {
		String	def;
		
		def = PropertiesHelper.systemHelper.getString(defaultSanityCheckThresholdNanosProperty, UndefinedAction.ZeroOnUndefined);
		if (def != null) {
			defaultSanityCheckThresholdNanos = Long.parseLong(def);
		} else {
			defaultSanityCheckThresholdNanos = defaultDefaultSanityCheckThresholdNanos; 
		}
		
		def = PropertiesHelper.systemHelper.getString(defaultOriginDeltaToleranceNanosProperty, UndefinedAction.ZeroOnUndefined);
		if (def != null) {
			defaultOriginDeltaToleranceNanos = Long.parseLong(def);
		} else {
			defaultOriginDeltaToleranceNanos = defaultDefaultOriginDeltaToleranceNanos; 
		}
    }
	
	public SafeAbsNanosTimeSource(long nanoOriginTimeInMillis, long sanityCheckThresholdNanos, long originDeltaToleranceNanos) {
		this.nanoOriginTimeInMillis = nanoOriginTimeInMillis;
		this.sanityCheckThresholdNanos = sanityCheckThresholdNanos;
		this.originDeltaToleranceNanos = originDeltaToleranceNanos;
		nanoOriginTimeInNanos = new AtomicLong();
		lastReturnedAbsTimeNanos = new AtomicLong();
		lastSystemNanos = new AtomicLong();
		setNanosOriginTime(System.nanoTime());
	}
	
	public SafeAbsNanosTimeSource(long nanoOriginTimeInMillis) {
		this(nanoOriginTimeInMillis, defaultSanityCheckThresholdNanos, defaultOriginDeltaToleranceNanos);
	}
	
	/**
	 * When a problem is detected, recompute nanoOriginTimeInMillis.
	 */
	private void setNanosOriginTime(long _systemTimeNanos) {
        long        curTimeMillis;
        long        curTimeNanos;
        long        deltaMillis;
        long        deltaNanos;
        long		newOriginTimeInNanos;
        
        curTimeMillis = System.currentTimeMillis();
        curTimeNanos = _systemTimeNanos;
        deltaMillis = curTimeMillis - nanoOriginTimeInMillis;
        if (deltaMillis < 0) {
        	System.err.printf("%d\n", curTimeMillis);
        	System.err.printf("%d\n", nanoOriginTimeInMillis);
        	throw new RuntimeException("deltaMillis < 0");
        }
        deltaNanos = deltaMillis * nanosPerMilli;
        newOriginTimeInNanos = curTimeNanos - deltaNanos;
        // FUTURE - put into mutex function new in Java 8
        if (Math.abs(nanoOriginTimeInNanos.get() - newOriginTimeInNanos) > originDeltaToleranceNanos) {
            if (debug) {
            	System.out.printf("nanoOriginTimeInNanos %d -> %d\n", nanoOriginTimeInNanos.get(), newOriginTimeInNanos);
            }
            nanoOriginTimeInNanos.set(newOriginTimeInNanos);
        }
        //System.out.printf("nanoOriginTimeInNanos\t%s\n", nanoOriginTimeInNanos);
	}

	@Override
	public long getNanosOriginTime() {
		return nanoOriginTimeInNanos.get();
	}
	
	@Override
	public long absTimeNanos() {
		long	t;
		long	prev;
		
		t = System.nanoTime() - nanoOriginTimeInNanos.get();
		prev = lastTimeNanos.getAndUpdate(x -> x < t ? t : x + 1);
		return t > prev ? t : prev + 1;
		//return System.nanoTime() - nanoOriginTimeInNanos.get();
	}
	
	/*
	@Override
	public long absTimeNanos() {
		long	candidateAbsTimeNanos;
		long	safeAbsTimeNanos;
		long	_systemNanoTime;
		long	_lastReturnedNanos;
		long	_lastSystemNanos;
		long	_nanoOriginTime;
		
		_lastReturnedNanos = lastReturnedAbsTimeNanos.get();
		_lastSystemNanos = lastSystemNanos.get();
		_systemNanoTime = System.nanoTime(); // must only retrieve system time once
											 // so that remaining logic is valid
		lastSystemNanos.set(_systemNanoTime);
		if (_systemNanoTime < _lastSystemNanos) {
			_systemNanoTime = _lastSystemNanos;
		}
		_nanoOriginTime = nanoOriginTimeInNanos.get();
		
		if (_systemNanoTime < _lastSystemNanos) {
			setNanosOriginTime(_systemNanoTime);
		} 
		
		candidateAbsTimeNanos = _systemNanoTime - _nanoOriginTime;
		if (candidateAbsTimeNanos - _lastReturnedNanos > sanityCheckThresholdNanos) {
			setNanosOriginTime(_systemNanoTime);
			candidateAbsTimeNanos = _systemNanoTime - nanoOriginTimeInNanos.get();
			
			if (candidateAbsTimeNanos < lastReturnedAbsTimeNanos.get()) {
				safeAbsTimeNanos = lastReturnedAbsTimeNanos.get();
			} else {
				safeAbsTimeNanos = candidateAbsTimeNanos;
				lastReturnedAbsTimeNanos.set(safeAbsTimeNanos);
			}			
		} else {
			safeAbsTimeNanos = candidateAbsTimeNanos; 
			lastReturnedAbsTimeNanos.set(safeAbsTimeNanos);
		}		
        return safeAbsTimeNanos;
	}
	*/

	/*
	@Override
	public long absTimeNanos() {
		long	candidateAbsTimeNanos;
		long	safeAbsTimeNanos;
		long	_systemNanoTime;
		
		_systemNanoTime = System.nanoTime(); // must only retrieve system time once
											 // so that remaining logic is valid
		candidateAbsTimeNanos = _systemNanoTime - nanoOriginTimeInNanos.get();
		if (candidateAbsTimeNanos - lastReturnedAbsTimeNanos.get() > sanityCheckThresholdNanos
				|| candidateAbsTimeNanos < lastReturnedAbsTimeNanos.get()) { // if we suspect something is wrong
			if (debug) {
				System.out.printf("%s\t%d\t%d\n", Thread.currentThread().getName(), candidateAbsTimeNanos, lastReturnedAbsTimeNanos.get());
				System.out.printf("%s\t%s\n", (candidateAbsTimeNanos - lastReturnedAbsTimeNanos.get() > sanityCheckThresholdNanos),
				(candidateAbsTimeNanos < lastReturnedAbsTimeNanos.get()));
			}
			setNanosOriginTime(_systemNanoTime);
			candidateAbsTimeNanos = _systemNanoTime - nanoOriginTimeInNanos.get();
			
			if (candidateAbsTimeNanos < lastReturnedAbsTimeNanos.get()) {
				safeAbsTimeNanos = lastReturnedAbsTimeNanos.get();
			} else {
				safeAbsTimeNanos = candidateAbsTimeNanos;
				lastReturnedAbsTimeNanos.set(safeAbsTimeNanos);
			}			
		} else {
			safeAbsTimeNanos = candidateAbsTimeNanos; 
			lastReturnedAbsTimeNanos.set(safeAbsTimeNanos);
		}		
        return safeAbsTimeNanos;
	}
	*/

	@Override
	public long relNanosRemaining(long absDeadlineNanos) {
        return TimeSourceUtil.relTimeRemainingAsInt(absDeadlineNanos, absTimeNanos());
	}
	
//	@Override
//	public int hashCode() {
//		return Long.hashCode(nanoOriginTimeInMillis) 
//				^ Long.hashCode(sanityCheckThresholdNanos)
//				^ Long.hashCode(originDeltaToleranceNanos);
////		return Long.hashCode(nanoOriginTimeInMillis) 
////				^ nanoOriginTimeInNanos.hashCode()
////				^ lastReturnedAbsTimeNanos.hashCode()
////				^ lastSystemNanos.hashCode()
////				^ Long.hashCode(sanityCheckThresholdNanos)
////				^ Long.hashCode(originDeltaToleranceNanos);
//	}
	
//	@Override
//	public boolean equals(Object o) {
//    	if (this == o) {
//    		return true;
//    	}
//    		
//    	if (this.getClass() != o.getClass()) {
//    		return false;
//    	}
//		
//		SafeAbsNanosTimeSource other = (SafeAbsNanosTimeSource)o;
//
//		return nanoOriginTimeInMillis == other.nanoOriginTimeInMillis
//				&& sanityCheckThresholdNanos == other.sanityCheckThresholdNanos
//				&& originDeltaToleranceNanos == other.originDeltaToleranceNanos;
////		return nanoOriginTimeInMillis == other.nanoOriginTimeInMillis
////				&& nanoOriginTimeInNanos.equals(other.nanoOriginTimeInNanos)
////				&& lastReturnedAbsTimeNanos.equals(other.lastReturnedAbsTimeNanos)
////				&& lastSystemNanos.equals(other.lastSystemNanos)
////				&& sanityCheckThresholdNanos == other.sanityCheckThresholdNanos
////				&& originDeltaToleranceNanos == other.originDeltaToleranceNanos;
//	}
	
	public static void main(String[] args) {
		SafeAbsNanosTimeSource	s;
		Timer					timer;
		long					t0;
		long					t1;
		
		timer = new StopwatchBasedTimer(new SimpleStopwatch(), TimeUnit.SECONDS, 120);		
		s = new SafeAbsNanosTimeSource(System.currentTimeMillis());
		t0 = s.absTimeNanos();
		do {
			t1 = s.absTimeNanos();
			if (t1 < t0) {
				System.out.printf("%d < %d\n", t1, t0);
			}
			t0 = t1;
		} while (!timer.hasExpired());
		/*
		SafeAbsNanosTimeSource	s;
		long					absTimeNanos;
		long					t1;
		long					_t3;
		
		t1 = System.nanoTime();
		s = new SafeAbsNanosTimeSource(System.currentTimeMillis());
		_t3 = t1;
		do {
			long	t2;
			long	t3;
			
			absTimeNanos = s.absTimeNanos();
			t2 = System.nanoTime();
			t3 = t2 - t1;
			if (t3 < 0 || (t3 - _t3) > defaultSanityCheckThresholdNanos) {
				System.out.printf("%d\t%d\n", t3, absTimeNanos);
			}
			_t3 = t3;
		} while (absTimeNanos < 24 * 60 * 60 * 1000 * nanosPerMilli);
		*/
	}
}

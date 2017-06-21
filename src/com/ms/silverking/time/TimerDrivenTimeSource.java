package com.ms.silverking.time;

import java.util.TimerTask;

import com.ms.silverking.util.SafeTimer;

/**
 * A RelNanosAbsMillisTimeSource that utilizes a Timer class to provide very 
 * low-overhead time reads. This timer is faster than SystemTimeSource, but 
 * is less accurate and more granular. 
 *
 */
public final class TimerDrivenTimeSource extends TimerTask implements RelNanosAbsMillisTimeSource {
	private final SafeTimer	timer;
	private volatile long	absTimeMillis;
	private volatile long	relTimeNanos;
	
	static final long	defaultPeriodMillis = 5;
	private static final String	defaultTimerNameBase = "TimerDriveTimeSource_";
	
	private static String defaultTimerName() {
		return defaultTimerNameBase + System.currentTimeMillis();
	}
	
	public TimerDrivenTimeSource(SafeTimer timer, long periodMillis) {
	    this.timer = timer;
		timer.scheduleAtFixedRate(this, 0, periodMillis);
	}

    public TimerDrivenTimeSource(SafeTimer timer) {
        this(timer, defaultPeriodMillis);
    }
    
    public TimerDrivenTimeSource(long periodMillis) {
        this(new SafeTimer(defaultTimerName(), true), periodMillis);
    }
    
	public TimerDrivenTimeSource() {
		this(defaultPeriodMillis);
	}
	
    public void stop() {
        timer.cancel();
    }
	
	@Override
	public long relTimeNanos() {
		return relTimeNanos;
	}

	@Override
	public long absTimeMillis() {
		return absTimeMillis;
	}
	
    @Override
    public int relMillisRemaining(long absDeadlineMillis) {
        return TimeSourceUtil.relTimeRemainingAsInt(absDeadlineMillis, absTimeMillis());
    }

	@Override
	public void run() {
		relTimeNanos = System.nanoTime();
		absTimeMillis = System.currentTimeMillis();
	}
}

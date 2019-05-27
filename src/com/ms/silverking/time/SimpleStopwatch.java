package com.ms.silverking.time;


/**
 * A simple concrete implementation of StopwatchBase that utilizes a 
 * RelNanosTimeSource and performs some basic sanity checks. 
 *
 */
public class SimpleStopwatch extends StopwatchBase {
	private final RelNanosTimeSource	relNanosTimeSource;
	
	protected SimpleStopwatch(RelNanosTimeSource relNanosTimeSource, long startTimeNanos) {
		super(startTimeNanos);
		this.relNanosTimeSource = relNanosTimeSource;
	}
	
	public SimpleStopwatch(RelNanosTimeSource relNanosTimeSource) {
		this(relNanosTimeSource, relNanosTimeSource.relTimeNanos());
	}
	
    public SimpleStopwatch() {
        this(SystemTimeSource.instance);
    }
    
	protected final long relTimeNanos() {
		return relNanosTimeSource.relTimeNanos();
	}
	
	// control
	
	@Override
	public void start() {
		ensureState(State.stopped);
		super.start();
	}
	
	@Override
	public void stop() {
		ensureState(State.running);
		super.stop();
	}
	
	// elapsed
	
	@Override
	public long getElapsedNanos() {
		ensureState(State.stopped);
		return super.getElapsedNanos();
	}	
}

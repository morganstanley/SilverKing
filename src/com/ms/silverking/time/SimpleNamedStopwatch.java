package com.ms.silverking.time;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Extends SimpleStopwatch with support for naming this Stopwatch.
 */
public class SimpleNamedStopwatch extends SimpleStopwatch {
	private final String	name;
	
    private static final String    genericNameBase = "SimpleStopwatch";
    private static final AtomicLong genericNameID = new AtomicLong();    
	
	public SimpleNamedStopwatch(RelNanosTimeSource relNanosTimeSource, String name) {
		super(relNanosTimeSource, relNanosTimeSource.relTimeNanos());
		this.name = name;
	}
	
	public SimpleNamedStopwatch(String name) {
		this(SystemTimeSource.instance, name);
	}
	
    public SimpleNamedStopwatch() {
        this(genericNameBase + genericNameID.getAndIncrement());
    }	
	
	@Override
	public String getName() {
		return name;
	}
}

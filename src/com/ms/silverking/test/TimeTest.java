package com.ms.silverking.test;

import java.math.BigDecimal;
import java.math.MathContext;

import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.RelNanosTimeSource;
import com.ms.silverking.time.SimpleNamedStopwatch;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.time.SystemTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;

public class TimeTest {
	public TimeTest() {
	}
	
	private static void displayRate(Stopwatch sw, int count) {
		BigDecimal	rate;
		BigDecimal	elapsed;
		
		elapsed = sw.getElapsedSecondsBD();
		rate = new BigDecimal(count).divide(elapsed, MathContext.DECIMAL128);
		System.out.printf("%40s\t%10.0f\t%e\n", sw.toStringElapsed(), 
				rate.doubleValue(), BigDecimal.ONE.divide(rate, MathContext.DECIMAL128).doubleValue());
	}
	
	public static void time() {
		Stopwatch	swSystem;
		Stopwatch	swNanos;
		Stopwatch	swTimerDriven;
		Stopwatch	swTimerDrivenDirect;
		TimerDrivenTimeSource ts;
		
		int			reps = 1000000;
		long		result;
		TimeTest	timeTest;
		RelNanosTimeSource	relNanosTimeSource;
		AbsMillisTimeSource	absMillisTimeSource;
		
		absMillisTimeSource = new SystemTimeSource();
		timeTest = new TimeTest();
		result = 0;
		swSystem = new SimpleNamedStopwatch("System");
		for (int i = 0; i < reps; i++) {
			//result += System.currentTimeMillis();
			result += absMillisTimeSource.absTimeMillis();
		}
		swSystem.stop();
		
		swNanos = new SimpleNamedStopwatch("Nanos");
		for (int i = 0; i < reps; i++) {
			result += System.nanoTime();
		}
		swNanos.stop();
		
		relNanosTimeSource = new TimerDrivenTimeSource();
		swTimerDriven = new SimpleNamedStopwatch("TimerDriven");
		for (int i = 0; i < reps; i++) {
			//System.out.println(timeTest.getElapsed());
			result += relNanosTimeSource.relTimeNanos();
		}
		swTimerDriven.stop();
		
		ts = new TimerDrivenTimeSource();
		swTimerDrivenDirect = new SimpleNamedStopwatch("TimerDrivenDirect");
		for (int i = 0; i < reps; i++) {
			//System.out.println(timeTest.getElapsed());
			result += ts.relTimeNanos();
		}
		swTimerDrivenDirect.stop();
		
		System.out.println(result +"\n");
		displayRate(swSystem, reps);
		displayRate(swNanos, reps);
		displayRate(swTimerDriven, reps);
		displayRate(swTimerDrivenDirect, reps);
	}
		
	public static void main(String[] args) {
		time();
	}
}

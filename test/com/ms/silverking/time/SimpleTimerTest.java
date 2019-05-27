package com.ms.silverking.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import static com.ms.silverking.testing.Assert.*;
import static com.ms.silverking.testing.Util.*;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import com.ms.silverking.time.Stopwatch.State;

public class SimpleTimerTest {

	private static final long   NANOS_PER_MILLI  = 1_000_000;
	private static final double NANOS_PER_SECOND = 1_000_000_000;
	
	private ExceptionChecker             startEc = new ExceptionChecker() { @Override public void check(){ startTimer(); } };
	private ExceptionChecker              stopEc = new ExceptionChecker() { @Override public void check(){  stopTimer(); } };
	private ExceptionChecker             resetEc = new ExceptionChecker() { @Override public void check(){ resetTimer(); } };
	
	private ExceptionChecker elapsedNanosEc      = new ExceptionChecker() { @Override public void check(){ getElapsedNanos();      } };
	private ExceptionChecker elapsedMillisLongEc = new ExceptionChecker() { @Override public void check(){ getElapsedMillisLong(); } };
	private ExceptionChecker elapsedMillisEc     = new ExceptionChecker() { @Override public void check(){ getElapsedMillis();     } };
	private ExceptionChecker elapsedSecondsEc    = new ExceptionChecker() { @Override public void check(){ getElapsedSeconds();    } };
	private ExceptionChecker elapsedSecondsBDEc  = new ExceptionChecker() { @Override public void check(){ getElapsedSecondsBD();  } };
	
	private SimpleTimer timer;
	private SimpleTimer exceptionTimer;
	private static final long DURATION_IN_SECONDS = 1;
	
	@Before
	public void setUp() throws Exception {
		createTimer();
	}
	
	private long nanos2millis(long nanos) {
		return nanos / NANOS_PER_MILLI;
	}
	
	private double nanos2seconds(double nanos) {
		return nanos / NANOS_PER_SECOND;
	}
	
	private BigDecimal seconds2BD(double seconds) {
		return new BigDecimal(seconds, MathContext.DECIMAL128);
//		return new BigDecimal(Double.toString(seconds), MathContext.DECIMAL128);
	}
	
	private void createTimer() {
		timer = new SimpleTimer(TimeUnit.SECONDS, DURATION_IN_SECONDS);
	}
	
	private void createExceptionTimer() {
		exceptionTimer = new SimpleTimer(TimeUnit.SECONDS, long_maxVal);
	}
	
	private void startTimer() {
		timer.start();
	}
	
	private void stopTimer() {
		timer.stop();
	}
	
	private void resetTimer() {
		timer.reset();
	}
	
	private long getElapsedNanos() {
		return timer.getElapsedNanos();
	}
	
	private long getElapsedMillisLong() {
		return timer.getElapsedMillisLong();
	}
	
	private int getElapsedMillis() {
		return timer.getElapsedMillis();
	}
	
	private double getElapsedSeconds() {
		return timer.getElapsedSeconds();
	}
	
	private BigDecimal getElapsedSecondsBD() {
		return timer.getElapsedSecondsBD();
	}
	
	private long getSplitNanos() {
		return timer.getSplitNanos();
	}
	
	private long getSplitMillisLong() {
		return timer.getSplitMillisLong();
	}
	
	private int getSplitMillis() {
		return timer.getSplitMillis();
	}
	
	private double getSplitSeconds() {
		return timer.getSplitSeconds();
	}
	
	private BigDecimal getSplitSecondsBD() {
		return timer.getSplitSecondsBD();
	}
	
	private long getRemainingNanos() {
		return timer.getRemainingNanos();
	}
	
	private long getRemainingMillisLong() {
		return timer.getRemainingMillisLong();
	}
	
	private int getRemainingMillis() {
		return timer.getRemainingMillis();
	}
	
	private double getRemainingSeconds() {
		return timer.getRemainingSeconds();
	}
	
	private BigDecimal getRemainingSecondsBD() {
		return timer.getRemainingSecondsBD();
	}
	
	private long getTimeLimitNanos() {
		return timer.getTimeLimitNanos();
	}
	
	private long getTimeLimitMillisLong() {
		return timer.getTimeLimitMillisLong();
	}
	
	private int getTimeLimitMillis() {
		return timer.getTimeLimitMillis();
	}
	
	private double getTimeLimitSeconds() {
		return timer.getTimeLimitSeconds();
	}
	
	private BigDecimal getTimeLimitSecondsBD() {
		return timer.getTimeLimitSecondsBD();
	}
	
	@Test
	public void testStart() {
		checkStartFails();
		stopTimer();
		checkStartPasses();
		checkStartFails();
	}

	private void checkStartFails() {
		exceptionConditionChecker(false, startEc);
	}
	
	private void checkStartPasses() {
		exceptionConditionChecker(true, startEc);
	}
	
	@Test
	public void testStop() {
		checkStopPasses();
		checkStopFails();
		startTimer();
		checkStopPasses();
	}
	
	private void checkStopFails() {
		exceptionConditionChecker(false, stopEc);
	}
	
	private void checkStopPasses() {
		exceptionConditionChecker(true, stopEc);
	}
	
	@Test
	public void testReset() {
//		checkResetPasses();
		stopTimer();
//		checkResetFails();
	}
	
	private void checkResetFails() {
		exceptionConditionChecker(false, resetEc);
	}
	
	private void checkResetPasses() {
		exceptionConditionChecker(true, resetEc);
	}
	
	@Test
	public void testGetElapsed() {
		checkElapsedNanosFails();
		checkElapsedMillisLongFails();
		checkElapsedMillisFails();
		checkElapsedSecondsFails();
		checkElapsedSecondsBDFails();
		stopTimer();
		
		checkNanos(getElapsedNanos(), getElapsedMillisLong(), getElapsedMillis(), getElapsedSeconds(), getElapsedSecondsBD());
	}
	
	private void checkElapsedNanosFails() {
		exceptionConditionChecker(false, elapsedNanosEc);
	}
	
	private void checkElapsedMillisLongFails() {
		exceptionConditionChecker(false, elapsedMillisLongEc);
	}
	
	private void checkElapsedMillisFails() {
		exceptionConditionChecker(false, elapsedMillisEc);
	}
	
	private void checkElapsedSecondsFails() {
		exceptionConditionChecker(false, elapsedSecondsEc);
	}
	
	private void checkElapsedSecondsBDFails() {
		exceptionConditionChecker(false, elapsedSecondsBDEc);
	}
	
	private void checkNanos(long nanos, long nanosMillis, int nanosMillisInt, double nanosSeconds, BigDecimal nanosSecondsBD) {
		long expectedNanosMillis    = nanos2millis( nanos);
		double expectedNanosSeconds = nanos2seconds(nanos);
		
		checkPositive(nanos);
		assertEquals(         expectedNanosMillis,   nanosMillis);
		assertEquals(    (int)expectedNanosMillis,   nanosMillisInt);
		assertEquals(         expectedNanosSeconds,  nanosSeconds, 0.001);
		assertEquals(round(seconds2BD(expectedNanosSeconds)), round(nanosSecondsBD));
	}
	
	private void checkPositive(long value) {
		assertTrue(value >= 0);
	}
	
	private BigDecimal round(BigDecimal bd) {
		return bd.round(new MathContext(3));
	}
	
	@Test
	public void testGetSplit() {
		stopTimer();

		long       splitNanos           = getSplitNanos();
		long       splitNanosMillisLong = getSplitMillisLong();
		int        splitNanosMillis     = getSplitMillis();
		double     splitNanosSeconds    = getSplitSeconds();
		BigDecimal splitNanosSecondsBD  = getSplitSecondsBD();

		checkNanos(splitNanos, splitNanosMillisLong, splitNanosMillis, splitNanosSeconds, splitNanosSecondsBD);

		// make sure splits >= elapsed
		long       elapsedNanos           = getElapsedNanos();
		long       elapsedNanosMillisLong = getElapsedMillisLong();
		int        elapsedNanosMillis     = getElapsedMillis();
		double     elapsedNanosSeconds    = getElapsedSeconds();
		BigDecimal elapsedNanosSecondsBD  = getElapsedSecondsBD();
		
		checkLessThanEqualTo(elapsedNanos,           splitNanos);
		checkLessThanEqualTo(elapsedNanosMillisLong, splitNanosMillisLong);
		checkLessThanEqualTo(elapsedNanosMillis,     splitNanosMillis);
		checkLessThanEqualTo(elapsedNanosSeconds,    splitNanosSeconds);
		checkLessThanEqualTo(elapsedNanosSecondsBD,  splitNanosSecondsBD);
		
		// make sure splits <= elapsed
		startTimer();
		splitNanos           = getSplitNanos();
		splitNanosMillisLong = getSplitMillisLong();
		splitNanosMillis     = getSplitMillis();
		splitNanosSeconds    = getSplitSeconds();
		splitNanosSecondsBD  = getSplitSecondsBD();
		stopTimer();
		
		elapsedNanos           = getElapsedNanos();
		elapsedNanosMillisLong = getElapsedMillisLong();
		elapsedNanosMillis     = getElapsedMillis();
		elapsedNanosSeconds    = getElapsedSeconds();
		elapsedNanosSecondsBD  = getElapsedSecondsBD();
		
		checkLessThanEqualTo(splitNanos,           elapsedNanos);
		checkLessThanEqualTo(splitNanosMillisLong, elapsedNanosMillisLong);
		checkLessThanEqualTo(splitNanosMillis,     elapsedNanosMillis);
		checkLessThanEqualTo(splitNanosSeconds,    elapsedNanosSeconds);
		checkLessThanEqualTo(splitNanosSecondsBD,  elapsedNanosSecondsBD);
	}
	
	private void checkLessThanEqualTo(long a, long b) {
		checkTrue(a <= b);
	}
	
	private void checkLessThanEqualTo(double a, double b) {
		checkTrue(a <= b);
	}
	
	private void checkLessThanEqualTo(BigDecimal a, BigDecimal b) {
		assertNotEquals(1, a.compareTo(b));
	}
	
	private void checkTrue(boolean condition) {
		assertTrue(condition);
	}

	@Test
	public void testGetName() {
		assertEquals("", timer.getName());
	}

	@Test
	public void testState() {
		checkStateRunning();
		checkRunningTrue();
		checkStoppedFalse();
		
		stopTimer();
		checkStateStopped();
		checkRunningFalse();
		checkStoppedTrue();
		
		startTimer();
//		resetTimer();
		stopTimer();
		startTimer();
		checkStateRunning();
		checkRunningTrue();
		checkStoppedFalse();
	}
	
	private void checkStateRunning() {
		checkState(State.running);
	}
	
	private void checkStateStopped() {
		checkState(State.stopped);
	}
	
	private void checkState(State expected) {
		assertEquals(expected, timer.getState());
	}
	
	private void checkRunningTrue() {
		checkRunning(true);
	}
	
	private void checkRunningFalse() {
		checkRunning(false);
	}
	
	private void checkRunning(boolean expected) {
		assertEquals(expected, timer.isRunning());
	}

	private void checkStoppedTrue() {
		checkStopped(true);
	}
	private void checkStoppedFalse() {
		checkStopped(false);
	}
	
	private void checkStopped(boolean expected) {
		assertEquals(expected, timer.isStopped());
	}

	@Test
	public void testToString() {
		String expected = ":running:";
		checkToString(expected);
		checkToStringSplit(expected);
		
		stopTimer();
		expected = ":stopped:";
		checkToString(expected);
		checkToStringSplit(expected);
		checkToStringElapsed(expected+""+getElapsedSeconds()+":1.0");
	}

	private void checkToString(String expected) {
		checkStartsWith(expected, timer.toString());
	}
	
	private void checkToStringSplit(String expected) {
		checkStartsWith(expected, timer.toStringSplit());
	}
	
	private void checkStartsWith(String expected, String actual) {
		assertTrue(actual.startsWith(expected));
	}
	
	private void checkToStringElapsed(String expected) {
		assertEquals(expected, timer.toStringElapsed());
	}
	
	@Test(expected=RuntimeException.class) 
	public void testGetRemainingMillis_Exception() {
		createExceptionTimer();
		exceptionTimer.getRemainingMillis();
	}
	
	@Test
	public void testExpiredAndRemaining() {
		assertFalse(timer.hasExpired());

		long nanos          = getRemainingNanos();
		double nanosSeconds = getRemainingSeconds();
		long splitNanos     = getSplitNanos();
		checkNanos(nanos, getRemainingMillisLong(), getRemainingMillis(), nanosSeconds, getRemainingSecondsBD());
		assertTrue(nanos > splitNanos);
		assertTrue(nanosSeconds < DURATION_IN_SECONDS);
		
		timer.waitForExpiration();
		
		assertTrue(timer.hasExpired());
		nanos = getRemainingNanos();
		assertEquals(0, nanos);
		checkNanos(nanos, getRemainingMillisLong(), getRemainingMillis(), getRemainingSeconds(), getRemainingSecondsBD());
	}
	
	@Test(expected=RuntimeException.class) 
	public void testGetTimeLimitMillis_Exception() {
		createExceptionTimer();
		exceptionTimer.getTimeLimitMillis();
	}
	
	@Test
	public void testGetTimeLimit() {
		double nanosSeconds = getTimeLimitSeconds();
		assertEquals(DURATION_IN_SECONDS, nanosSeconds, 0);
		checkNanos(getTimeLimitNanos(), getTimeLimitMillisLong(), getTimeLimitMillis(), nanosSeconds, getTimeLimitSecondsBD());
	}
}

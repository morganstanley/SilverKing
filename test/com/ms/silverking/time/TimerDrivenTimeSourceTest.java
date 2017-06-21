package com.ms.silverking.time;

import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_SecondNotEqualsThird;
import static com.ms.silverking.time.TestUtil.timerSource;
import static com.ms.silverking.time.TestUtil.test_AbsTimeMillis;
import static com.ms.silverking.time.TestUtil.test_RelMillisRemaining;

import static com.ms.silverking.time.TimerDrivenTimeSource.defaultPeriodMillis;

import org.junit.Test;

public class TimerDrivenTimeSourceTest {

	private static final long defaultperiodMillisCopy = 5;
	private static final long defaultperiodMillisDiff = 6;
	
	private static final TimerDrivenTimeSource timerSourceCopy =  new TimerDrivenTimeSource(defaultperiodMillisCopy);
	private static final TimerDrivenTimeSource timerSourceDiff =  new TimerDrivenTimeSource(defaultperiodMillisDiff);

	// ALL OF THIS IS COMMENTED OUT because the getters use variables that are dynamic, so too hard to test...
	
//	@Test
//	public void testAbsTimeMillis() {
//		Object[][] testCases = {
//			{defaultPeriodMillis,     timerSource},
//			{defaultperiodMillisCopy, timerSourceCopy},
//			{defaultperiodMillisDiff, timerSourceDiff},
//		};
//		
//		test_AbsTimeMillis(testCases);
//	}
//	
//	@Test
//	public void testRelMillisRemaining() {
//		Object[][] testCases = {
//			{ minus( 1L, timerSource.absTimeMillis()), timerSource,      1L},
//			{ minus(-1L, timerSource.absTimeMillis()), timerSourceCopy, -1L},
//			{ minus( 0L, timerSource.absTimeMillis()), timerSourceDiff,  1L},
//		};
//		
//		test_RelMillisRemaining(testCases);
//	}
//	
//	private long minus(long a, long b) {
//		return a-b;
//	}
//	
//	@Test
//	public void testRelTimeNanos() {
//		Object[][] testCases = {
//			{ 1L, constantSource,      1L},
//			{-1L, constantSourceCopy, -1L},
//			{ 0L, constantSourceDiff,  1L},
//		};
//		
//		test_RelTimeNanos(testCases);
//	}
	
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   timerSource, timerSource);
		checkHashCodeNotEquals(timerSource, timerSourceCopy);	// SafeTimer - uses object identity for hashCode
		checkHashCodeNotEquals(timerSource, timerSourceDiff);
	}
	
	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{timerSource,     timerSource,     timerSourceDiff},
			{timerSourceDiff, timerSourceDiff, timerSource},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{timerSource, TestUtil.constantSource},
			{timerSource, TestUtil.systemSource},
			{timerSource, timerSourceCopy},	// SafeTimer compares using identity
		});
	}
}

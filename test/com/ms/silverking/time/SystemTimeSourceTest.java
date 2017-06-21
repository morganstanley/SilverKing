package com.ms.silverking.time;

import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static com.ms.silverking.testing.AssertFunction.test_FirstEqualsSecond_SecondNotEqualsThird;
import static com.ms.silverking.time.TestUtil.*;
import static com.ms.silverking.time.TestUtil.test_AbsTimeMillis;
import static com.ms.silverking.time.TestUtil.test_RelMillisRemaining;
import static org.junit.Assert.*;

import org.junit.Test;

public class SystemTimeSourceTest {

	private static final long absNanosTimeCopy = 0;
	private static final long absNanosTimeDiff = 1;
	
	private static final SystemTimeSource systemSourceCopy =  SystemTimeSource.createWithMillisOrigin(absNanosTimeCopy);
	private static final SystemTimeSource systemSourceDiff =  SystemTimeSource.createWithMillisOrigin(absNanosTimeDiff);

	
	// ALL OF THIS IS COMMENTED OUT because the getters use variables that are dynamic, so too hard to test...
	
	
//	@Test
//	public void testAbsTimeMillis() {
//		Object[][] testCases = {
//			{absMillisTime,     systemSource},
//			{absMillisTimeCopy, systemSourceCopy},
//			{absMillisTimeDiff, systemSourceDiff},
//		};
//		
//		test_AbsTimeMillis(testCases);
//	}
	
//	@Test
//	public void testRelMillisRemaining() {
//		Object[][] testCases = {
//			{ 1L, systemSource,      1L},
//			{-1L, systemSourceCopy, -1L},
//			{ 0L, systemSourceDiff,  1L},
//		};
//		
//		test_RelMillisRemaining(testCases);
//	}
	
	
//	@Test
//	public void testRelTimeNanos() {
//		Object[][] testCases = {
//			{absMillisTime,     systemSource},
//			{absMillisTimeCopy, systemSourceCopy},
//			{absMillisTimeDiff, systemSourceDiff},
//		};
//		
//		test_AbsTimeMillis(testCases);
//	}
	

//	@Test
//	public void testAbsTimeNanos() {
//		Object[][] testCases = {
//			{absNanosTime,     systemSource},
////			{absNanosTimeCopy, systemSourceCopy},
////			{absNanosTimeDiff, systemSourceDiff},
//		};
//		
//		test_AbsTimeNanos(testCases);
//	}
	
//	@Test
//	public void testRelNanosRemaining() {
//		Object[][] testCases = {
//			{ 1L, systemSource,      1L},
//			{-1L, systemSourceCopy, -1L},
//			{ 0L, systemSourceDiff,  1L},
//		};
//		
//		test_RelMillisRemaining(testCases);
//	}
	
//	@Test
//	public void testGetNanosOriginTime() {
//		Object[][] testCases = {
//			{absNanosTime,     systemSource},
//			{absNanosTimeCopy, systemSourceCopy},
//			{absNanosTimeDiff, systemSourceDiff},
//		};
//		
//		test_GetNanosOriginTime(testCases);
//	}
	
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   systemSource, systemSource);
		checkHashCodeNotEquals(systemSource, systemSourceCopy);
		checkHashCodeNotEquals(systemSource, systemSourceDiff);
	}
	
	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{systemSource,     systemSource,     systemSourceDiff},
			{systemSourceDiff, systemSourceDiff, systemSource},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{systemSource, TestUtil.constantSource},
			{systemSource, TestUtil.timerSource},
			{systemSource, systemSourceCopy},
		});
	}
}

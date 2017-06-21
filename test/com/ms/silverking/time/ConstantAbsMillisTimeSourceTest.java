package com.ms.silverking.time;

import static com.ms.silverking.testing.AssertFunction.*;
import static com.ms.silverking.time.TestUtil.test_AbsTimeMillis;
import static com.ms.silverking.time.TestUtil.*;

import org.junit.Test;

public class ConstantAbsMillisTimeSourceTest {

	private static final long absMillisTimeCopy = 0;
	private static final long absMillisTimeDiff = 1;
	
	private static final ConstantAbsMillisTimeSource constantSourceCopy = new ConstantAbsMillisTimeSource(absMillisTimeCopy);
	private static final ConstantAbsMillisTimeSource constantSourceDiff = new ConstantAbsMillisTimeSource(absMillisTimeDiff);

	@Test
	public void testAbsTimeMillis() {
		Object[][] testCases = {
			{absMillisTime,     constantSource},
			{absMillisTimeCopy, constantSourceCopy},
			{absMillisTimeDiff, constantSourceDiff},
		};
		
		test_AbsTimeMillis(testCases);
	}
	
	@Test
	public void testRelMillisRemaining() {
		Object[][] testCases = {
			{ 1L, constantSource,      1L},
			{-1L, constantSourceCopy, -1L},
			{ 0L, constantSourceDiff,  1L},
		};
		
		test_RelMillisRemaining(testCases);
	}
	
	@Test
	public void testHashCode() {
		checkHashCodeEquals(   constantSource, constantSource);
		checkHashCodeEquals(   constantSource, constantSourceCopy);
		checkHashCodeNotEquals(constantSource, constantSourceDiff);
	}
	
	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{constantSource,     constantSource,     constantSourceDiff},
			{constantSourceDiff, constantSourceDiff, constantSource},
			{constantSourceCopy, constantSource,     constantSourceDiff},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{constantSource, TestUtil.systemSource},
			{constantSource, TestUtil.timerSource},
		});
	}
}

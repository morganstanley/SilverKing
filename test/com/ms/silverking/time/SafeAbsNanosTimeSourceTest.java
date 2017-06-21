package com.ms.silverking.time;

import static com.ms.silverking.testing.AssertFunction.*;

import org.junit.Test;

public class SafeAbsNanosTimeSourceTest {

	private static final long absNanosTime     = 0;
	private static final long absNanosTimeCopy = 0;
	private static final long absNanosTimeDiff = 1;
	
	private static final SafeAbsNanosTimeSource safeSource     = new SafeAbsNanosTimeSource(absNanosTime);
	private static final SafeAbsNanosTimeSource safeSourceCopy = new SafeAbsNanosTimeSource(absNanosTimeCopy);
	private static final SafeAbsNanosTimeSource safeSourceDiff = new SafeAbsNanosTimeSource(absNanosTimeDiff);

	@Test
	public void testHashCode() {
		checkHashCodeEquals(   safeSource, safeSource);
		checkHashCodeNotEquals(safeSource, safeSourceCopy);	// hashcode uses identity
		checkHashCodeNotEquals(safeSource, safeSourceDiff);
	}
	
	@Test
	public void testEqualsObject() {
		Object[][] testCases = {
			{safeSource,     safeSource,     safeSourceDiff},
			{safeSourceDiff, safeSourceDiff, safeSource},
		};
		
		test_FirstEqualsSecond_SecondNotEqualsThird(testCases);
		test_NotEquals(new Object[][]{
			{safeSource, TestUtil.systemSource},
			{safeSource, safeSourceCopy},	// equals uses identity
		});
	}
}

package com.ms.silverking.time;

import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimeSourceUtilTest {

	@Test(expected=IllegalArgumentException.class)
	public void testRelTimeRemainingAsInt_Overflow() {
		TimeSourceUtil.relTimeRemainingAsInt(4_000_000_000L, 0L);
	}

	@Test
	public void testRelTimeRemainingAsInt() {
		Object[][] testCases = {
			{0L, 0L, 0},
			{1L, 0L, 1},
			{1_000_000_000L, 0L, 1_000_000_000},
		};
		
		for (Object[] testCase : testCases) {
			long deadline         = (long)testCase[0];
			long currentTime      = (long)testCase[1];
			int expectedRemaining =  (int)testCase[2];
			
			checkRelTimeRemainingAsInt(deadline, currentTime, expectedRemaining);
		}
	}
	
	private void checkRelTimeRemainingAsInt(long deadline, long currentTime, int expectedRemaining) {
		assertEquals( getTestMessage("relTimeRemainingAsInt", deadline, currentTime), expectedRemaining, TimeSourceUtil.relTimeRemainingAsInt(deadline, currentTime));
	}

}

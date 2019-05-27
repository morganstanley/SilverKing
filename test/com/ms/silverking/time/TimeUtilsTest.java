package com.ms.silverking.time;

import static org.junit.Assert.assertEquals;

import static com.ms.silverking.testing.Util.getTestMessage;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;

public class TimeUtilsTest {

	@Test
	public void testInMillis() {
		int[][] testCases = {
			{-348, -1_252_800_000},
			{-10,     -36_000_000},
			{-5,      -18_000_000},
			{-1,       -3_600_000},
			{0,                 0},
			{1,         3_600_000},
			{5,        18_000_000},
			{10,       36_000_000},
			{348,   1_252_800_000},
		};
		
		for (int[] testCase : testCases) {
			int hours          = testCase[0];
			int expectedMillis = testCase[1];
			
			int minutes = hours*60;
			int seconds = minutes*60;
			
			checkHoursInMillis(hours, expectedMillis);
			checkMinutesInMillis(minutes, expectedMillis);
			checkSecondsInMillis(seconds, expectedMillis);
		}
	}
	
	private void checkHoursInMillis(int hours, int expected) {
		assertEquals( getTestMessage("hoursInMillis", hours), expected, TimeUtils.hoursInMillis(hours));
	}

	private void checkMinutesInMillis(int minutes, int expected) {
		assertEquals( getTestMessage("minutesInMillis", minutes), expected, TimeUtils.minutesInMillis(minutes));
	}
	
	private void checkSecondsInMillis(int seconds, int expected) {
		assertEquals( getTestMessage("secondInMillis", seconds), expected, TimeUtils.secondsInMillis(seconds));
	}

}

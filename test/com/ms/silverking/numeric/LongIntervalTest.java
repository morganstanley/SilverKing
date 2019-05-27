package com.ms.silverking.numeric;

import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.*;

import org.junit.Test;

public class LongIntervalTest {

	@Test(expected=IllegalArgumentException.class)
	public void testConstructor_Exception() {
		createInterval(1, 0);
	}
	
	@Test
	public void testGettersAndToString() {
		long[][] testCases = {
			{0, 0, 1},
			{0, 1, 2},
			{-4_123_456_789L, 7_568_123, 4_131_024_913L},
		};
		
		for (long[] testCase : testCases) {
			long start = testCase[0];
			long end   = testCase[1];
			long size  = testCase[2];
			
			LongInterval interval = createInterval(start, end);
			checkGetter("getStart", interval, start, interval.getStart());
			checkGetter("getEnd",   interval,   end, interval.getEnd());
			checkGetter("getSize",  interval,  size, interval.getSize());
			assertEquals( getTestMessage("toString"), "["+start+","+end+"]", interval.toString());
		}
	}
	
	private void checkGetter(String name, LongInterval interval, long expected, long actual) {
		assertEquals( getTestMessage(name, interval), expected, actual); 
	}
	
	@Test
	public void testEquals() {
		LongInterval zeroZero = createInterval(0, 0);
		LongInterval zeroOne  = createInterval(0, 1);
		LongInterval oneOne   = createInterval(1, 1);

		Object[][] testCases = {
			{zeroZero, zeroZero,  true,  true},
			{zeroOne,  zeroZero, false, false},
			{zeroOne,   zeroOne,  true,  true},
			{zeroOne,    oneOne, false, false},
			
			{zeroZero, createInterval(0, 0),  true,  true},
			{createInterval(-4_123_456_789L, 7_568_123), createInterval(-4_123_456_789L, 7_568_123), true, true},
		};
		
		for (Object[] testCase : testCases) {
			LongInterval l1      = (LongInterval)testCase[0];
			LongInterval l2      = (LongInterval)testCase[1];
			boolean expectedL1L2 =      (boolean)testCase[2];
			boolean expectedL2L1 =      (boolean)testCase[3];

			checkEquals(l1, l2, expectedL1L2);
			checkEquals(l2, l1, expectedL2L1);
		}
	}
	
	private LongInterval createInterval(long l1, long l2) {
		return new LongInterval(l1, l2);
	}
	
	private void checkEquals(LongInterval l1, LongInterval l2, boolean expected) {
		assertEquals( getTestMessage("equals", l1, l2), expected, l1.equals(l2)); 
	}
}

package com.ms.silverking.numeric;

import static com.ms.silverking.testing.Assert.exceptionNameChecker;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class RingIntegerTest {

	private RingInteger ri;
	private static final int min = -1000;
	private static final int max = 1000;
	private static final int value = 0;
	
	@Before
	public void setUp() throws Exception {
		ri = createInteger(min, max, value);
	}
	
	private RingInteger createInteger(int min, int max, int value) {
		return new RingInteger(min, max, value);
	}

	@Test
	public void testConstructor_Exceptions() {
		int[][] testCases = {
			{0, -1,  0},  
			{0, -1, -2},  
			{0,  1,  2},  
			{-2_123_456_789, 7_568_123, 21_548_444},
		};
			
		for (int[] testCase : testCases) {
			int min   = testCase[0];
			int max   = testCase[1];
			int value = testCase[2];
			
			String testMessage = getTestMessage("RingInteger_Exceptions", min, max, value); 
			ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ createInteger(min, max, value); } };
			exceptionNameChecker(ec, testMessage, IllegalArgumentException.class);
		}
	}

	@Test
	public void testIncrementAndDecrementAndValue() {
		for (int i = value; i < max; i++) {
			checkGetValue(i);
			checkHashCode(i);
			ri.increment();
		}
		
		for (int i = max; i > -min; i--) {
			checkGetValue(i);
			checkHashCode(i);
			ri.decrement();
		}
		
		// test overflow wrap-arounds
		ri = createInteger(min, max, max);
		checkGetValue(max);
		checkHashCode(max);
		ri.increment();
		checkGetValue(min);
		checkHashCode(min);
		ri.decrement();
		checkGetValue(max);
		checkHashCode(max);
	}
	
	private void checkGetValue(int expected) {
		assertEquals(expected, ri.getValue());
	}
	
	private void checkHashCode(int expected) {
		assertEquals(expected, ri.hashCode());
	}
	
	@Test
	public void testEqualsAndRingShared() {
		RingInteger zeroZeroZero = createInteger(0, 0, 0);
		RingInteger zeroOneZero  = createInteger(0, 1, 0);
		RingInteger zeroOneOne   = createInteger(0, 1, 1);
		RingInteger oneOneOne    = createInteger(1, 1, 1);

		Object[][] testCases = {
			{zeroZeroZero, zeroZeroZero,  true,  true},
			{zeroZeroZero,  zeroOneZero,  true, false},
			{zeroZeroZero,   zeroOneOne, false, false},
			{zeroZeroZero,    oneOneOne, false, false},
			{oneOneOne,     zeroOneZero, false, false},
			{oneOneOne,      zeroOneOne,  true, false},
			{oneOneOne,       oneOneOne,  true,  true},

			{zeroOneZero,       zeroOneOne,  false,  true},
			{zeroZeroZero, createInteger(0, 0, 0),  true,  true},
			{createInteger(-2_123_456_789, 85_567_010, 21_548_444), createInteger(-2_123_456_789, 85_567_010, 7_568_123), false, true},
		};
		
		for (Object[] testCase : testCases) {
			RingInteger r1             = (RingInteger)testCase[0];
			RingInteger r2             = (RingInteger)testCase[1];
			boolean expectedEquals     =     (boolean)testCase[2];
			boolean expectedRingShared =     (boolean)testCase[3];

			checkEquals(r1, r2, expectedEquals);
			checkRingShared(r2, r1, expectedRingShared);
		}
	}
	
	private void checkEquals(RingInteger r1, RingInteger r2, boolean expected) {
		assertEquals( getTestMessage("equals", r1, r2), expected, r1.equals(r2)); 
	}
	
	private void checkRingShared(RingInteger r1, RingInteger r2, boolean expected) {
		assertEquals( getTestMessage("ringShared", r1, r2), expected, RingInteger.ringShared(r1, r2)); 
	}

}

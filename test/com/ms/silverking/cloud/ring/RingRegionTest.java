package com.ms.silverking.cloud.ring;

import static org.junit.Assert.*;

import org.junit.Test;

public class RingRegionTest {

	private static final RingRegion zeroZero   = new RingRegion(0, 0);
	private static final RingRegion zeroOne    = new RingRegion(0, 1);
	private static final RingRegion oneOne     = new RingRegion(1, 1);
	private static final RingRegion twoTwo     = new RingRegion(2, 2);
	private static final RingRegion fullRing   = new RingRegion(LongRingspaceTest.start, LongRingspaceTest.end);
	private static final RingRegion endToStart = new RingRegion(LongRingspaceTest.end, LongRingspaceTest.start);
//	private static final RingRegion endToStartExpanded = new RingRegion(LongRingspaceTest.end+1, LongRingspaceTest.start+1);

	@Test
	public void testGetSize() {
		Object[][] testCases = {
			{zeroZero,                       1L},
			{zeroOne,                        2L},
			{fullRing,   LongRingspaceTest.size},
			{endToStart,                     2L},	// is 2 right?
		};
			
		for (Object[] testCase : testCases) {
			RingRegion region = (RingRegion)testCase[0];
			long expected     =       (long)testCase[1];

			assertEquals(expected, region.getSize());
		}
	}
	
	@Test
	public void testBeforeAndAfter() {
		Object[][] testCases = {
			{zeroZero,  0L,  0L, false, false},
			{zeroZero,  0L,  1L, true,  false},
			{zeroZero,  1L,  1L, false, false},
			{zeroOne,   0L,  0L, false, false},
			{zeroOne,   0L,  1L, true,  false},
			{zeroOne,   1L,  1L, false, false},
			{zeroOne,   2L,  2L, false, false},
			{zeroOne,  -1L, -1L, false, false},
			{zeroOne,  -1L,  0L, false, true},
			{zeroOne,   0L, -1L, true,  false},
		};
			
		for (Object[] testCase : testCases) {
			RingRegion region      = (RingRegion)testCase[0];
			long p0                =       (long)testCase[1];
			long p1                =       (long)testCase[2];
			boolean expectedBefore =    (boolean)testCase[3];
			boolean expectedAfter  =    (boolean)testCase[4];

			assertEquals("testBefore", expectedBefore, region.before(p0, p1));
			assertEquals("testAfter",  expectedAfter,  region.after(p0, p1));
		}
	}
	
	
	
	@Test
	public void testGetRingspaceFraction() {
//		Object[][] testCases = {
//			{zeroZero,  zeroZero},
//			{zeroZero,  new RingRegion(0, 0)},
//		};
//			
//		for (Object[] testCase : testCases) {
//			RingRegion region1 = (RingRegion)testCase[0];
//			RingRegion region2 = (RingRegion)testCase[1];
//
//			assertEquals(region1, region2);
//		}
	}
	
	
	
	@Test
	public void testEquals() {
		Object[][] testCases = {
			{zeroZero,  zeroZero},
			{zeroZero,  new RingRegion(0, 0)},
		};
			
		for (Object[] testCase : testCases) {
			RingRegion region1 = (RingRegion)testCase[0];
			RingRegion region2 = (RingRegion)testCase[1];

			assertEquals(region1, region2);
		}
	}
	
	@Test
	public void testContains() {
		Object[][] testCases = {
			{zeroZero,                         -1L, false},
			{zeroZero,                          0L, true},
			{zeroZero,                          1L, false},
			{zeroOne,                          -1L, false},
			{zeroOne,                           0L, true},
			{zeroOne,                           1L, true},
			{zeroOne,                           2L, false},
			{fullRing,   LongRingspaceTest.start-1, false},
			{fullRing,   LongRingspaceTest.start,   true},
			{fullRing,   LongRingspaceTest.start+1, true},
			{fullRing,   LongRingspaceTest.end-1,   true},
			{fullRing,   LongRingspaceTest.end,     true},
			{fullRing,   LongRingspaceTest.end+1,   false},
			{endToStart, LongRingspaceTest.end-1,   false},
			{endToStart, LongRingspaceTest.end,     true},
			{endToStart, LongRingspaceTest.end+1,   true},// ??
			{endToStart, LongRingspaceTest.start-1, true},// ??
			{endToStart, LongRingspaceTest.start,   true},
			{endToStart, LongRingspaceTest.start+1, false},
		};
			
		for (Object[] testCase : testCases) {
			RingRegion region1 = (RingRegion)testCase[0];
			long point         =       (long)testCase[1];
			boolean expected   =    (boolean)testCase[2];

			assertEquals(expected, region1.contains(point));
		}
	}
	
	@Test
	public void testOverlaps() {
		Object[][] testCases = {
			{zeroZero,           zeroZero,           true},
			{zeroZero,           zeroOne,            true},
			{zeroOne,            zeroZero,           true},
			{zeroOne,            oneOne,             true},
			{zeroOne,            twoTwo,             false},
			{endToStart,         zeroZero,           false},
//			{endToStart,         endToStartExpanded, true},
//			{endToStartExpanded, endToStart,         true},
		};
			
		for (Object[] testCase : testCases) {
			RingRegion region1 = (RingRegion)testCase[0];
			RingRegion region2 = (RingRegion)testCase[1];
			boolean expected   =    (boolean)testCase[2];

			assertEquals(expected, region1.overlaps(region2));
		}
	}

}

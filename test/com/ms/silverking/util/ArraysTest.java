package com.ms.silverking.util;

import static com.ms.silverking.testing.Util.copy;
import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Assert.exceptionNameChecker;
import static com.ms.silverking.testing.Util.getTestMessage;
import static com.ms.silverking.testing.Util.sort;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class ArraysTest {

	private static final byte[] zero   = {0};
	private static final byte[] one    = {1};
	private static final byte[] oneTwo = {1, 2};
	private static final byte[] negative2Positive = {-128, -5, -2, -1, 0, 1, 2, 5, 127};
	private static final byte[] negative = {-128, -5, -2, -1};
	private static final byte[] positive = {0, 1, 2, 5, 127};
	
	private static final int[] zeroI   = {0};
	private static final int[] oneI    = {1};
	private static final int[] oneTwoI = {1, 2};
	private static final int[] negative2PositiveI = {-128, -5, -2, -1, 0, 1, 2, 5, 127};
	
	private static final Integer[] zeroInt   = {0};
	private static final Integer[] oneInt    = {1};
	private static final Integer[] oneTwoInt = {1, 2};
	
	private static final String[] zeroStr   = {"0"};
	private static final String[] oneStr    = {"1"};
	private static final String[] oneTwoStr = {"1", "2"};

	@Test
	public void testMatchesRegion() {
		Object[][] testCases = {
			{zero,                            0, zero,                   0,                        0,  true},
			{zero,                            0, zero,                   0,              zero.length,  true},
			{zero,                            0, one,                    0,                        0,  true},
			{zero,                            0, one,                    0,              zero.length, false},
			{one,                             0, zero,                   0,               one.length, false},
			{one,                             0, oneTwo,                 0,               one.length,  true},
			{one,                             0, oneTwo,                 1,               one.length, false},
			
			{negative2Positive,               0, negative2Positive,      0, negative2Positive.length,  true},
			{negative2Positive,               0, negative,               0,          negative.length,  true},
			{negative,                        0, negative2Positive,      0,          negative.length,  true},
			{negative2Positive, negative.length, positive,               0,          positive.length,  true},
			{positive,                        0, negative2Positive, negative.length,          positive.length,  true},
		};
		
		for (Object[] testCase : testCases) {
			byte[] a1        =  (byte[])testCase[0];
			int offset1      =     (int)testCase[1];
			byte[] a2        =  (byte[])testCase[2];
			int offset2      =     (int)testCase[3];
			int length       =     (int)testCase[4];
			boolean expected = (boolean)testCase[5];
			
			checkMatchesRegion(a1, offset1, a2, offset2, length, expected);
		}
	}
	
	private void checkMatchesRegion(byte[] a1, int offset1, byte[] a2, int offset2, int length, boolean expected) {
		assertEquals( getTestMessage("matchesRegion", 
				createToString(a1), 
				offset1, 
				createToString(a1), 
				offset2, 
				length), 
				expected, Arrays.matchesRegion(a1, offset1, a2, offset2, length));
	}
	
	@Test
	public void testMatchesStart_Exceptions() {
		Object[][] testCases = {
			{oneTwo, 	         one,      ArrayIndexOutOfBoundsException.class},
			{negative2Positive,  negative, ArrayIndexOutOfBoundsException.class},
		};
		
		for (Object[] testCase : testCases) {
			byte[] a1                       =   (byte[])testCase[0];
			byte[] a2                       =   (byte[])testCase[1];
			Class<?> expectedExceptionClass = (Class<?>)testCase[2];

			String testMessage = getTestMessage("matchesStart_Exceptions", createToString(a1), createToString(a2)); 
			ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkMatchesStart(a1, a2, false); } };
			exceptionNameChecker(ec, testMessage, expectedExceptionClass);
		}
	}
	
	@Test
	public void testMatchesStart() {
		Object[][] testCases = {
			{zero,     zero,               true},
			{zero,     one,               false},
			{one,      zero,              false},
			{one,      oneTwo,             true},
			{negative, negative2Positive,  true},
			{negative, positive,          false},
		};
		
		for (Object[] testCase : testCases) {
			byte[] a1        =  (byte[])testCase[0];
			byte[] a2        =  (byte[])testCase[1];
			boolean expected = (boolean)testCase[2];
			
			checkMatchesStart(a1, a2, expected);
		}
	}
	
	private void checkMatchesStart(byte[] a1, byte[] a2, boolean expected) {
		assertEquals( getTestMessage("matchesStart", createToString(a1), createToString(a2)),	expected, Arrays.matchesStart(a1, a2));
	}
	
	@Test
	public void testIndexofAndContains() {
		Object[][] testCases = {
			{zeroInt,    -1, -1, false},
			{zeroInt,     0,  0,  true},
			{zeroInt,     1, -1, false},
			{oneInt,      0, -1, false},
			{oneInt,      1,  0,  true},
			{oneInt,      2, -1, false},
			{oneTwoInt,   0, -1, false},
			{oneTwoInt,   1,  0,  true},
			{oneTwoInt,   2,  1,  true},
			{oneTwoInt,   3, -1, false},
			{zeroStr,    -1, -1, false},
			{zeroStr,  "-1", -1, false},
			{zeroStr,     0, -1, false},
			{zeroStr,   "0",  0,  true},
			{zeroStr,     1, -1, false},
			{zeroStr,   "1", -1, false},
			{oneStr,    "0", -1, false},
			{oneStr,    "1",  0,  true},
			{oneStr,    "2", -1, false},
			{oneTwoStr, "0", -1, false},
			{oneTwoStr, "1",  0,  true},
			{oneTwoStr, "2",  1,  true},
			{oneTwoStr, "3", -1, false},
		};
		
		for (Object[] testCase : testCases) {
			Object[] a                = (Object[])testCase[0];
			Object value              =   (Object)testCase[1];
			int expectedIndex         =      (int)testCase[2];
			boolean expectedContains  =  (boolean)testCase[3];
			
			checkIndexOf( a, value, expectedIndex);
			checkContains(a, value, expectedContains);
		}
	}
	
	private <T> void checkIndexOf(T[] a, T value, int expectedIndex) {
		assertEquals( getTestMessage("indexOf", createToString(a), value), expectedIndex, Arrays.indexOf(a, value));
	}
	
	private <T> void checkContains(T[] a, T value, boolean expected) {
		assertEquals( getTestMessage("contains", createToString(a), value), expected, Arrays.contains(a, value));
	}
	
	@Test
	public void testShuffleIntArray() {
		int[][] testCases = {
			zeroI,
			oneI,
			oneTwoI,
			negative2PositiveI,
		};
		
		for (int[] testCase : testCases) 
			checkShuffleIntArray(testCase);
	}
	
	private void checkShuffleIntArray(int[] a) {
		int[] origCopy = copy(a);
		int[] actualSorted = copy(a);
		Arrays.shuffleIntArray(actualSorted);
		sort(actualSorted);
		assertArrayEquals( getTestMessage("shuffleIntArray: orig vs shuffled+sorted",
				"orig = " + createToString(origCopy),
				"shuffled+sorted = " + createToString(actualSorted)),
				origCopy, actualSorted);
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testRandomUniqueIntArray_MinEqualsMax() {
		Arrays.randomUniqueIntArray(0, 0);
	}
	
	@Test
	public void testRandomUniqueIntArray() {
		Object[][] testCases = {
			{0,  1, zeroI},
			{0,  2, new int[]{0, 1}},
			{1,  2, oneI},
			{1,  3, oneTwoI},
			{1, 10, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}},
		};
		
		for (Object[] testCase : testCases) {
			int min        =   (int)testCase[0];
			int max        =   (int)testCase[1];
			int[] expected = (int[])testCase[2];
			
			checkRandomUniqueIntArray(min, max, expected);
		}
	}
	
	private void checkRandomUniqueIntArray(int min, int max, int[] expected) {
		int[] actual = Arrays.randomUniqueIntArray(min, max);
		int[] actualSorted = copy(actual);
		sort(actualSorted);
		assertArrayEquals( getTestMessage("randomUniqueIntArray",
				"min = " + min,
				"max = " + max,
				"expected = " + createToString(expected),
				"actualSorted = " + createToString(actualSorted)),
				expected, actualSorted);
	}
}

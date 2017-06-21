package com.ms.silverking.collection;

import static com.ms.silverking.testing.Util.getTestMessage;
import static com.ms.silverking.collection.TestUtil.expectedDoubleValue;
import static com.ms.silverking.collection.TestUtil.expectedIntValue;
import static com.ms.silverking.collection.TestUtil.expectedIntValue2;
import static com.ms.silverking.collection.TestUtil.expectedStringValue;
import static com.ms.silverking.collection.TestUtil.pD;
import static com.ms.silverking.collection.TestUtil.pI;
import static com.ms.silverking.collection.TestUtil.pS;
import static com.ms.silverking.collection.TestUtil.pSI;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TripleTest {

	public static final Triple<Integer, Integer, Integer>  tI                       = new Triple<>(expectedIntValue,    expectedIntValue,    expectedIntValue);
	public static final Triple<Integer, Integer, Integer>  tI_OneThirdCopy1         = new Triple<>(expectedIntValue,    expectedIntValue2,   expectedIntValue2);
	public static final Triple<Integer, Integer, Integer>  tI_OneThirdCopy2         = new Triple<>(expectedIntValue2,   expectedIntValue,    expectedIntValue2);
	public static final Triple<Integer, Integer, Integer>  tI_OneThirdCopy3         = new Triple<>(expectedIntValue2,   expectedIntValue2,   expectedIntValue);
	public static final Triple<Integer, Integer, Integer>  tI_TwoThirdCopy1         = new Triple<>(expectedIntValue2,   expectedIntValue,    expectedIntValue);
	public static final Triple<Integer, Integer, Integer>  tI_TwoThirdCopy2         = new Triple<>(expectedIntValue,    expectedIntValue2,   expectedIntValue);
	public static final Triple<Integer, Integer, Integer>  tI_TwoThirdCopy3         = new Triple<>(expectedIntValue,    expectedIntValue,    expectedIntValue2);
	public static final Triple<Double, Double, Double>     tD                       = new Triple<>(expectedDoubleValue, expectedDoubleValue, expectedDoubleValue);
	public static final Triple<String, String, String>     tS                       = new Triple<>(expectedStringValue, expectedStringValue, expectedStringValue);
	public static final Triple<Integer, String, Integer>   tISI                     = new Triple<>(expectedIntValue,    expectedStringValue, expectedIntValue);
	public static final Triple<Triple<Integer, String, Integer>, String, String> tM = new Triple<>(tISI,                expectedStringValue, expectedStringValue);
	public static final Triple<Integer, Integer, Integer> tPI1 = Triple.of(pI, expectedIntValue);
	public static final Triple<Integer, Integer, Integer> tPI2 = Triple.of(expectedIntValue, pI);
	
	@Test
	public void testGet() {
		Object[][] testCases = {
			{tI,   expectedIntValue,    expectedIntValue,    expectedIntValue},
			{tD,   expectedDoubleValue, expectedDoubleValue, expectedDoubleValue},
			{tS,   expectedStringValue, expectedStringValue, expectedStringValue},
			{tISI, expectedIntValue,    expectedStringValue, expectedIntValue},
			{tM,   tISI,                expectedStringValue, expectedStringValue },
		};
			
		for (Object[] testCase : testCases) {
			Triple<?, ?, ?> t = (Triple<?, ?, ?>) testCase[0];
			Object expectedV1 =                   testCase[1];
			Object expectedV2 =                   testCase[2];
			Object expectedV3 =                   testCase[3];

			checkV1(t, expectedV1);
			checkV2(t, expectedV2);
			checkV3(t, expectedV3);
		}
	}
	
	private void checkV1(Triple<?, ?, ?> t, Object expectedValue) {
		assertEquals( getTestMessage("v1", t), expectedValue, t.getV1());
	}

	private void checkV2(Triple<?, ?, ?> t, Object expectedValue) {
		assertEquals( getTestMessage("v2", t), expectedValue, t.getV2());
	}

	private void checkV3(Triple<?, ?, ?> t, Object expectedValue) {
		assertEquals( getTestMessage("v3", t), expectedValue, t.getV3());
	}
	
	@Test
	public void testEquals_Valid() {
		Object[][] testCases = {
			{tI,   tI,               true},
			{tI,   tI_OneThirdCopy1, false},
			{tI,   tI_OneThirdCopy2, false},
			{tI,   tI_OneThirdCopy3, false},
			{tI,   tI_TwoThirdCopy1, false},
			{tI,   tI_TwoThirdCopy2, false},
			{tI,   tI_TwoThirdCopy3, false},
			{tI,   tD,               false},
			{tS,   tD,               false},
			{tPI1, tPI2,              true},
		};
			
		for (Object[] testCase : testCases) {
			Triple<?, ?, ?> t1 = (Triple<?, ?, ?>)testCase[0];
			Triple<?, ?, ?> t2 = (Triple<?, ?, ?>)testCase[1];
			boolean expected   =         (boolean)testCase[2];

			checkEquals(t1, t2, expected);
		}
	}
	
	private void checkEquals(Triple<?, ?, ?> t1, Triple<?, ?, ?> t2, boolean expected) {
		assertEquals( getTestMessage("Triples are equal", t1, t2), expected, t1.equals(t2));
	}
	
	@Test
	public void testHead() {
		Object[][] testCases = {
			{tI,      expectedIntValue},
			{tD,   expectedDoubleValue},
			{tS,   expectedStringValue},
			{tISI,    expectedIntValue},
			{tPI1,    expectedIntValue},
			{tPI2,    expectedIntValue},
			{tM,                  tISI},
		};
			
		for (Object[] testCase : testCases) {
			Triple<?, ?, ?> t = (Triple<?, ?, ?>)testCase[0];
			Object expected   =                  testCase[1];

			checkHead(t, expected);
		}
	}

	private void checkHead(Triple<?, ?, ?> t, Object expectedValue) {
		assertEquals( getTestMessage("Head", t), expectedValue, t.getHead());
	}
	
	@Test
	public void testTail() {
		Object[][] testCases = {
			{tI,    pI},
			{tD,    pD},
			{tS,    pS},
			{tISI, pSI},
			{tPI1,  pI},
			{tPI2,  pI},
			{tM,    pS},
		};
			
		for (Object[] testCase : testCases) {
			Triple<?, ?, ?> t   = (Triple<?, ?, ?>)testCase[0];
			Pair<?, ?> expected =      (Pair<?, ?>)testCase[1];

			checkTail(t, expected);
		}
	}
	
	private void checkTail(Triple<?, ?, ?> t, Pair<?, ?> expected) {
		assertEquals( getTestMessage("Tail", t), expected, t.getTail());
	}
}

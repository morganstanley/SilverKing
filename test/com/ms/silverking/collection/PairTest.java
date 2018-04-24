package com.ms.silverking.collection;

import static com.ms.silverking.collection.TestUtil.expectedDoubleValue;
import static com.ms.silverking.collection.TestUtil.expectedIntValue;
import static com.ms.silverking.collection.TestUtil.expectedIntValue2;
import static com.ms.silverking.collection.TestUtil.expectedStringValue;
import static com.ms.silverking.collection.TestUtil.pD;
import static com.ms.silverking.collection.TestUtil.pI;
import static com.ms.silverking.collection.TestUtil.pIS;
import static com.ms.silverking.collection.TestUtil.pM;
import static com.ms.silverking.collection.TestUtil.pS;
import static com.ms.silverking.collection.TestUtil.pSI;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PairTest {

	private static final Pair<Integer, Integer> pI_HalfCopy = new Pair<>(expectedIntValue, expectedIntValue2);
	private static final Pair<Integer, Integer> pI_FullCopy = new Pair<>(expectedIntValue, expectedIntValue);
	
	@Test
	public void testGet() {
		Object[][] testCases = {
			{pI,  expectedIntValue,    expectedIntValue},
			{pD,  expectedDoubleValue, expectedDoubleValue},
			{pS,  expectedStringValue, expectedStringValue},
			{pIS, expectedIntValue,    expectedStringValue},
			{pSI, expectedStringValue, expectedIntValue},
			{pM,  pIS,                 expectedStringValue},
		};
			
		for (Object[] testCase : testCases) {
			Pair<?, ?> p      = (Pair<?, ?>)testCase[0];
			Object expectedV1 =             testCase[1];
			Object expectedV2 =             testCase[2];

			checkV1(p, expectedV1);
			checkV2(p, expectedV2);
		}
	}
	
	private void checkV1(Pair<?, ?> p, Object expectedValue) {
		assertEquals( getTestMessage("v1", p), expectedValue, p.getV1());
	}
	
	private void checkV2(Pair<?, ?> p, Object expectedValue) {
		assertEquals( getTestMessage("v2", p), expectedValue, p.getV2());
	}
	
	@Test(expected=ClassCastException.class)
	public void testEquals_Invalid() {
		pM.equals(pD);
	}
	
	@Test
	public void testEquals_Valid() {
		Object[][] testCases = {
			{pI, pI,           true},
			{pI, pI_HalfCopy, false},
			{pI, pI_FullCopy,  true},
			{pI, pD,          false},
			{pS, pD,          false},
		};
			
		for (Object[] testCase : testCases) {
			Pair<?, ?> p1    = (Pair<?, ?>)testCase[0];
			Pair<?, ?> p2    = (Pair<?, ?>)testCase[1];
			boolean expected =    (boolean)testCase[2];

			checkEquals(p1, p2, expected);
		}
	}
	
	private void checkEquals(Pair<?, ?> p1, Pair<?, ?> p2, boolean expected) {
		assertEquals( getTestMessage("pairs are equal", p1, p2), expected, p1.equals(p2));
	}
}

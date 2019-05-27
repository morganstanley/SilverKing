package com.ms.silverking.id;

import static com.ms.silverking.testing.Util.getTestMessage;
import static com.ms.silverking.collection.TestUtil.expectedDoubleValue;
import static com.ms.silverking.collection.TestUtil.expectedIntValue;
import static com.ms.silverking.collection.TestUtil.expectedStringValue;
import static org.junit.Assert.*;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.ms.silverking.collection.Triple;

public class UUIDBaseTest {

	private static final UUID uuid_0_1 = new UUID(1, 0);
	
	@Test
	public void testUUIDConstructors() {
		new UUIDBase();
		new UUIDBase(false);
		new UUIDBase( UUID.randomUUID() );
		new UUIDBase(uuid_0_1);
		assertTrue(true);
	}

	@Test
	public void testGetSignificantBits() {
		long[][] testCases = {
			{0, 0},
			{0, 1},
			{1, 0},
			{4_123_456_789L, -4_123_456_789L},
		};
			
		for (long[] testCase : testCases) {
			long msb = testCase[0];
			long lsb = testCase[1];

			UUIDBase uuidBase = new UUIDBase(msb, lsb);
			checkMostSignificant(uuidBase, msb);
			checkLeastSignificant(uuidBase, lsb);
		}
	}
	
	private void checkMostSignificant(UUIDBase base, long expectedMsb) {
		assertEquals( getTestMessage("getMostSignificantBits", base), expectedMsb, base.getMostSignificantBits());
	}
	
	private void checkLeastSignificant(UUIDBase base, long expectedLsb) {
		assertEquals( getTestMessage("getLeastSignificantBits", base), expectedLsb, base.getLeastSignificantBits());
	}
	
	@Test
	public void testGetUUID() {
		UUIDBase uuidBase = new UUIDBase(uuid_0_1);
		assertEquals(uuid_0_1, uuidBase.getUUID());
	}

	@Test
	public void testCompareToAndEquals() {
		Object[][] testCases = {
			{0L, 0L, 0L, 0L,  0,  true},
			{0L, 1L, 0L, 0L,  1, false},
			{0L, 0L, 0L, 1L, -1, false},
			{0L, 1L, 1L, 0L, -1, false},
			{1L, 0L, 0L, 1L,  1, false},
			{1L, 1L, 1L, 0L,  1, false},
		};
			
		for (Object[] testCase : testCases) {
			long msb1              =    (long)testCase[0];
			long lsb1              =    (long)testCase[1];
			long msb2              =    (long)testCase[2];
			long lsb2              =    (long)testCase[3];
			int expectedCompareTo  =     (int)testCase[4];
			boolean expectedEquals = (boolean)testCase[5];

			UUIDBase uuidBase1 = new UUIDBase(msb1, lsb1);
			UUIDBase uuidBase2 = new UUIDBase(msb2, lsb2);
			checkCompareTo(uuidBase1, uuidBase2, expectedCompareTo);
			checkEquals(uuidBase1, uuidBase2, expectedEquals);
		}
	}
	
	private void checkCompareTo(UUIDBase base1, UUIDBase base2, int expectedValue) {
		assertEquals( getTestMessage("compareTo", base1, base2), expectedValue, base1.compareTo(base2));
	}
	
	private void checkEquals(UUIDBase base1, UUIDBase base2, boolean expectedValue) {
		assertEquals( getTestMessage("equals", base1, base2), expectedValue, base1.equals(base2));
	}
}

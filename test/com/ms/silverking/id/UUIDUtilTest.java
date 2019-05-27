package com.ms.silverking.id;

import static com.ms.silverking.testing.Util.createToString;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.junit.Test;

public class UUIDUtilTest {

    public static final int BYTES_PER_UUID = 16;
    
	@Test
	public void testUuidToBytes() {
		byte[] expectedZeros   = new byte[BYTES_PER_UUID];
		byte[] expectedPryamid = {0,   0,  0,  0,  0,  1, -30,  64, 0, 0, 0, 0,  0,   9, -5, -15};
		byte[] expectedMax     = {-1, -1, -1, -1, 10, 57,  10, -21, 0, 0, 0, 2, 76, -80, 22, -22};
		
		Object[][] testCases = {
			{             0L,             0L, expectedZeros},
			{        123456L,        654321L, expectedPryamid},
			{-4_123_456_789L, 9_876_543_210L, expectedMax},
		};
			
		for (Object[] testCase : testCases) {
			long msb     =   (long)testCase[0];
			long lsb     =   (long)testCase[1];
			byte[] bytes = (byte[])testCase[2];

			UUID uuid = new UUID(msb, lsb);
			checkUuidToBytes(uuid, bytes);
			checkBytesToUuid(bytes, uuid);
			checkGetUuid(ByteBuffer.wrap(bytes), uuid);
		}
	}
	
	private void checkUuidToBytes(UUID uuid, byte[] expected) {
		byte[] actual = UUIDUtil.uuidToBytes(uuid);
		assertArrayEquals( getTestMessage("checkUuidToBytes", uuid,
				"expected = " + createToString(expected),
				"actual   = " + createToString(actual)),
				expected, actual);
	}
	
	private void checkBytesToUuid(byte[] bytes, UUID expected) {
		assertEquals( getTestMessage("checkBytesToUuid", createToString(bytes)), expected, UUIDUtil.bytesToUUID(bytes));
	}
	
	private void checkGetUuid(ByteBuffer buff, UUID expected) {
		assertEquals( getTestMessage("checkGetUuid", buff), expected, UUIDUtil.getUUID(buff));
	}
}

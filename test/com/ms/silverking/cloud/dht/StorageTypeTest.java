package com.ms.silverking.cloud.dht;

import static org.junit.Assert.*;

import org.junit.Test;

import static com.ms.silverking.cloud.dht.StorageType.*;

public class StorageTypeTest {

	@Test
	public void testIsFileBased() {
		Object[][] testCases = {
			{RAM,      false},
			{FILE,      true},
			{FILE_SYNC, true},
		};
		
		for (Object[] testCase : testCases) {
			StorageType type = (StorageType)testCase[0];
			boolean expected =     (boolean)testCase[1];
			
			assertEquals(expected, type.isFileBased());
		}
	}

}

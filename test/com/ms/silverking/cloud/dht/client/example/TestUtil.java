package com.ms.silverking.cloud.dht.client.example;

import static org.junit.Assert.assertEquals;

import com.ms.silverking.testing.Util;

public class TestUtil {
	
	public static int[][] getFibTestCases() {
		return new int[][] {
			{1,        1},
			{10,      55},
			{30, 832_040},
		};
	}
	
	public static void checkValueIs(String expected, String actual) {
		assertEquals(expected, actual);
	}
}

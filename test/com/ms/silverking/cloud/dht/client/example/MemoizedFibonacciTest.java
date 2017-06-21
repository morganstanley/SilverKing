package com.ms.silverking.cloud.dht.client.example;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.testing.Util;

import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class MemoizedFibonacciTest {

	@Test
	public void testFibonacci() throws ClientException, IOException {
		int[][] testCases = {
			{1,        1},
			{10,      55},
			{30, 832_040},
		};

		MemoizedFibonacci fib = new MemoizedFibonacci( Util.getTestGridConfig() );
		for (int[] testCase : testCases) {
			int n           = testCase[0];
			int expectedFib = testCase[1];
			assertEquals(expectedFib, fib.fibonacci(n));
		}
	}
}

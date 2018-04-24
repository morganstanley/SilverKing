package com.ms.silverking.cloud.dht.client.example;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class AsyncMemoizedFibonacciTest {

	@Test
	public void testFibonacci() throws ClientException, IOException {
		AsyncMemoizedFibonacci fib = new AsyncMemoizedFibonacci( Util.getTestGridConfig() );
		for (int[] testCase : TestUtil.getFibTestCases()) {
			int n           = testCase[0];
			int expectedFib = testCase[1];
			assertEquals(expectedFib, fib.fibonacci(n));
		}
	}
}

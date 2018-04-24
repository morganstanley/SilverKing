package com.ms.silverking.cloud.dht.client.apps.test;

import java.io.IOException;

import org.junit.Test;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class WriteReadTestTest {

	@Test
	public void testDoTest() throws ClientException, IOException {
		int[][] testCases = {
			{1,       1,     1},
			{1,      20,   200},
			{17,     20,   200},
			{25,    100,   100},
//			{25,  1_000, 1_000},
//			{100, 1_000, 1_000},
		};
		
		for (int[] testCase : testCases) {
			int numThreads    = testCase[0];
			int keysPerThread = testCase[1];
			int updatesPerKey = testCase[2];
			WriteReadTest writeReadTest = new WriteReadTest( Util.getTestGridConfig(), System.out, System.err, numThreads, keysPerThread, updatesPerKey );
			writeReadTest.doTest();
		}
	}
	
	public static void main(String[] args) {
		Util.runTests(WriteReadTestTest.class);
	}

}

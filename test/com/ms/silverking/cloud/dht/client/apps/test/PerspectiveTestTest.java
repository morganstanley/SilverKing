package com.ms.silverking.cloud.dht.client.apps.test;

import java.io.IOException;

import org.junit.Test;

import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class PerspectiveTestTest {

	@Test
	public void testCreatePerspectives() throws ClientException, IOException {
		PerspectiveTest perspective = new PerspectiveTest( Util.getTestGridConfig(), System.out, System.err );
		int[] testCases = {1, 10, 100, 1_000};
		for (int testCase : testCases) {
			int numPerspectives = testCase;
			perspective.createPerspectives(numPerspectives);
		}
	}
	
	public static void main(String[] args) {
		Util.runTests(PerspectiveTestTest.class);
	}

}

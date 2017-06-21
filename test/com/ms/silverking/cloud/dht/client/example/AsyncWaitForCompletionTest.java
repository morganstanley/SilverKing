package com.ms.silverking.cloud.dht.client.example;

import java.io.IOException;

import org.junit.Test;

import com.ms.silverking.testing.Util;
import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class AsyncWaitForCompletionTest {

	@Test
	public void testWaitForCompletion() throws IOException {
		TestUtil.checkValueIs("value.1", AsyncWaitForCompletion.runExample( Util.getTestGridConfig() ));
	}
	
	public static void main(String[] args) {
		Util.runTests(AsyncWaitForCompletionTest.class);
	}

}

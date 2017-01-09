package com.ms.silverking.cloud.dht.client.example;

import static org.junit.Assert.*;

import org.junit.Test;

public class HelloEmbeddedDHTTest {

	@Test
	public void testDhts() {
		checkHello(HelloEmbeddedDHT.test());
		checkHello(HelloEmbeddedDHT2.test());
	}
	
	private void checkHello(String actual) {
		String expected = "world!";
		assertEquals(expected, actual);
	}
}

package com.ms.silverking.cloud.dht.client.example;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.testing.Util;

import com.ms.silverking.testing.annotations.SkLarge;

@SkLarge
public class HelloWorldTest {

	private static SKGridConfiguration gridConfig;

	@BeforeClass
	public static void setUpBeforeClass() throws ClientException, IOException {
		gridConfig = Util.getTestGridConfig();
	}

	@Test
	public void testDht() {
		TestUtil.checkValueIs("world!", HelloDHT.runExample(gridConfig));
	}

	@Test
	public void testEmbeddedDht() {
		TestUtil.checkValueIs("embedded world!", HelloEmbeddedDHT.runExample());
	}
	
	@Test
	public void testEmbeddedDht2() {
		TestUtil.checkValueIs("embedded2 world!", HelloEmbeddedDHT2.runExample());
	}
	
	@Test
	public void testObject() {
		TestUtil.checkValueIs("object world!", (String)HelloObject.runExample(gridConfig));
	}
	
	@Test
	public void testByteArray() {
		TestUtil.checkValueIs("byte[] world!", new String(HelloByteArray.runExample(gridConfig)));
	}
	
	@Test
	public void testAsyncDht() {
		TestUtil.checkValueIs("async world!", HelloAsyncDHT.runExample(gridConfig));
	}
	
	@Test
	public void testMap() {
		Map<String, String> expectedMap = ImmutableMap.of(
			"George Washington", "1789-1797", 
            "John Adams",        "1797-1801",
            "Thomas Jefferson",  "1801-1809",
            "James Madison",     "1809-1817",
            "James Monroe",      "1817-1825"
        );
		checkValueIs(expectedMap, HelloMap.runExample(gridConfig));
	}
	
	private void checkValueIs(Map<String, String> expected, Map<String,String> actual) {
		assertEquals(expected, actual);
	}
}

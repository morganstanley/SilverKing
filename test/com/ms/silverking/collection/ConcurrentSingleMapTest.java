package com.ms.silverking.collection;

import static com.ms.silverking.testing.Assert.checkEqualsEmptySet;
import static com.ms.silverking.testing.Assert.checkEqualsSetOne;
import static com.ms.silverking.collection.TestUtil.key1;
import static com.ms.silverking.collection.TestUtil.value1;
import static com.ms.silverking.collection.TestUtil.value2;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

public class ConcurrentSingleMapTest {
	
	private ConcurrentSingleMap<Integer, Integer> map;

	@Before
	public void setUp() throws Exception {
		clearMap();
	}
	
	private void clearMap() {
		map = new ConcurrentSingleMap<>();
	}
	
	@Test
	public void testEmpty() {
		assertTrue(map.isEmpty());
		checkSize(0);
		checkContainsKey(null, false);
		checkContainsValue(null, false);
		assertNull(map.get(null));
		checkEqualsEmptySet(map.keySet());
		checkEqualsEmptySet(map.values());
		checkPutReturnsNull(null, null);
//		checkEqualsEmptySet(map.keySet());
//		checkEqualsEmptySet(map.values());
	}
	
	@Test
	public void testPut() {
		checkPutReturnsNull(key1, value2);
		checkContainsKey(key1, true);
		checkContainsValue(value2, true);
		assertEquals(value2, map.get(key1).intValue());
	}
	
	@Test(expected=RuntimeException.class)
	public void testPut_AlreadySet() {
		putElement_1_1();
		putElement_1_2();
	}
	
	@Test
	public void testKeySet() {
		checkEqualsEmptySet(map.keySet());
		putElement_1_1();
		checkEqualsSetOne(map.keySet());
	}
	
	@Test
	public void testValues() {
		checkEqualsEmptySet(map.values());
		putElement_1_1();
		checkEqualsSetOne(map.values());
	}
	
	@Test
	public void testEntrySet() {
		checkEqualsEmptySet(map.entrySet());
		// maybe should add more here
	}
	
	private void putElement_1_1() {
		map.put(key1, value1);
	}
	
	private void putElement_1_2() {
		map.put(key1, value2);
	}
	
	private void checkSize(int expectedSize) {
		assertEquals(expectedSize, map.size());
	}
	
	private void checkContainsKey(Object key, boolean expected) {
		assertEquals(expected, map.containsKey(key));
	}
	
	private void checkContainsValue(Object value, boolean expected) {
		assertEquals(expected, map.containsValue(value));
	}
	
	private void checkPutReturnsNull(Integer key, Integer value) {
		assertNull(map.put(key, value));
	}
}

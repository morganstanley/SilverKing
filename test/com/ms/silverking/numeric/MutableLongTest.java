package com.ms.silverking.numeric;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class MutableLongTest {

	private MutableLong mi;
	
	@Before
	public void setUp() throws Exception {
		mi = new MutableLong(0);
	}

	@Test
	public void testMutableLong() {
		for (long i = 0; i < 1000; i++) {
			checkGetValue(i);
			checkHashCode(i);
			mi.increment();
		}
		
		for (long i = 1000; i > -1000; i--) {
			checkGetValue(i);
			checkHashCode(i);
			mi.decrement();
		}
		
		// test overflow wrap-arounds
		mi = new MutableLong(Long.MAX_VALUE);
		checkGetValue(Long.MAX_VALUE);
		checkHashCode(-1L);
		mi.increment();
		checkGetValue(Long.MIN_VALUE);
		checkHashCode(0L);
		mi.decrement();
		checkGetValue(Long.MAX_VALUE);
		checkHashCode(-1L);
		
		mi = new MutableLong(Integer.MAX_VALUE);
		checkGetValue(Integer.MAX_VALUE);
		checkHashCode(Integer.MAX_VALUE);
		mi.getAndIncrement();
		checkGetValue(Integer.MAX_VALUE+1L);
		checkHashCode(Integer.MIN_VALUE);
		mi.getAndDecrement();
		checkGetValue(Integer.MAX_VALUE);
		checkHashCode(Integer.MAX_VALUE);
	}
	
	private void checkGetValue(long expected) {
		assertEquals(expected, mi.getValue());
	}
	
	private void checkHashCode(long expected) {
		assertEquals(expected, mi.hashCode());
	}

}

package com.ms.silverking.collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

public class ConcurrentLinkedDequeWithSizeTest {

	private ConcurrentLinkedDequeWithSize<Integer> deque;

	@Before
	public void setUp() throws Exception {
		clearDeque();
	}
	
	private void clearDeque() {
		deque = new ConcurrentLinkedDequeWithSize<>();
	}
	
	@Test
	public void testEmpty() {
		assertTrue(deque.isEmpty());
		checkSize(0);
		checkPeek(null);
	}
	
	@Test(expected=NoSuchElementException.class)
	public void testEmptyRemove() {
		deque.remove();
	}
	
	@Test
	public void testSequence_AddPush() {
		int front = 1;
		int back  = front;
		assertTrue(deque.add(front));
		int expectedSize = 1;
		checkSize(expectedSize);
		checkPeek(front);
		
		front = 2;
		deque.push(front);
		expectedSize = 2;
		checkSize(expectedSize);
		checkPeek(front);
		checkRemove(front);
		
		front = back;
		back = 2;
		assertTrue(deque.add(back));
		checkSize(expectedSize);
		checkPeek(front);
		checkRemove(front);

		// should be 1,2,3
		front = 1;
		back = 3;
		deque.push(front);
		assertTrue(deque.add(back));
		checkSize(3);
		checkPeek(front);
		checkRemove(front);
	}
	
	private void checkSize(int expectedSize) {
		assertEquals(expectedSize, deque.size());
	}
	
	private void checkPeek(Integer expectedValue) {
		assertEquals(expectedValue, deque.peek());
	}
	
	private void checkRemove(Integer expectedValue) {
		assertEquals(expectedValue, deque.remove());
	}

}

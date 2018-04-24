package com.ms.silverking.testing;

import static com.ms.silverking.testing.Util.createList;
import static com.ms.silverking.testing.Util.createSet;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import com.ms.silverking.testing.Util.ExceptionChecker;

public class Assert {
	
	public static void exceptionConditionChecker(boolean expected, ExceptionChecker ec) {
		boolean started = true;
		try {
			ec.check();	
		}
		catch (RuntimeException re) {
			started = false;
		}
		
		assertEquals(expected, started);
	}
	
	public static void exceptionNameChecker(ExceptionChecker ec, String testMessage, Class<?> expected) {
		try {
			ec.check();
			fail(testMessage);
		} catch (Exception ex) {
			checkException(testMessage, expected, ex);
		}
	}
	
	public static void checkException(String testMessage, Class<?> expected, Exception ex) {
		assertEquals(testMessage, expected, ex.getClass());
	}

	public static void assertPass(String msg) {
		assertTrue(msg, true);
	}
	
	public static void assertZero(int actualSize) {
		assertEquals(0, actualSize);
	}

	public static <T> void checkEqualsEmptySet(Collection<T> actual) {
		assertEquals(createSet(), actual);
	}
	
	public static <T> void checkEqualsSetOne(Collection<T> actual) {
		assertEquals(createSet(1), actual);
	}
	
	public static <T> void checkEqualsEmptyList(Collection<T> actual) {
		assertEquals(createList(), actual);
	}
}

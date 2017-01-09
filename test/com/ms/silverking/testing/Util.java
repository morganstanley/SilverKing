package com.ms.silverking.testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

public class Util {

	public interface ExceptionChecker {
		public void check();
	}
	
	public static final byte     byte_minVal =    Byte.MIN_VALUE;
	public static final byte     byte_maxVal =    Byte.MAX_VALUE;
	public static final long     long_minVal =    Long.MIN_VALUE;
	public static final long     long_maxVal =    Long.MAX_VALUE;
	public static final int       int_minVal = Integer.MIN_VALUE;
	public static final int       int_maxVal = Integer.MAX_VALUE;
	public static final double double_maxVal =  Double.MAX_VALUE;

	public static final double double_nan    = Double.NaN;
	public static final double double_negInf = Double.NEGATIVE_INFINITY;
	public static final double double_posInf = Double.POSITIVE_INFINITY;
	
	public static String getTestMessage(String testName, Object... params) {
		String message = "\nChecking " + testName + ":\n";
		for (Object param : params) {
			message += param + "\n";
		}
		
		return message;
	}
	
	public static void pass() {
		assertTrue(true);
	}

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
	
	
	
	
	public static byte[] copy(byte[] a) {
		return copy(a, a.length);
	}
	
	public static byte[] copy(byte[] a, int length) {
		return Arrays.copyOf(a, length);
	}
	
	public static int[] copy(int[] a) {
		return copy(a, a.length);
	}
	
	public static int[] copy(int[] a, int length) {
		return Arrays.copyOf(a, length);
	}
	
	public static void sort(int[] a) {
		Arrays.sort(a);
	}

	@SafeVarargs
	public static <T> List<T> createList(T... elements) {
		return Arrays.asList(elements);
	}

//	if we get rid of all these usages of this method and inline them with the commented out portion that will work.
//  if we use this method with the commented out portion, we get these outputs instead of the actual contents of the array: [[I@579bb367] 
//	it has to do with primitives and generics:
//	   if we are using the method inline, we are calling it directly with the primitive array, like int[] a, so it works just fine
//	   if we are using this method, we are using a generic T, and passing createString(a), so it's getting a little confused
	@SafeVarargs
	public static <T> String createToString(T... elements) {
//		return Arrays.toString(elements);
		return Arrays.deepToString(elements);
	}
}

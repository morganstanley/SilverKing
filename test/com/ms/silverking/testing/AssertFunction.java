package com.ms.silverking.testing;

import static com.ms.silverking.testing.Util.getTestMessage;
import static com.ms.silverking.testing.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import com.ms.silverking.cloud.dht.client.crypto.AESEncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.XOREncrypterDecrypter;
import com.ms.silverking.testing.Util.ExceptionChecker;

public class AssertFunction {

	public static void test_Getters(Object[][] testCases) {
		for (Object[] testCase : testCases) 
			check_Getter(testCase[0], testCase[1]);
	}
	
	public static void check_Getter(Object expected, Object actual) {
		assertEquals(expected, actual);
	}
	
	public static void test_Setters(Object[][] testCases) {
		for (Object[] testCase : testCases) 
			check_Setter(testCase[0], testCase[1]);
	}
	
	public static void check_Setter(Object expected, Object actual) {
		assertEquals(expected, actual);
	}

	public static void test_SetterExceptions(Object[][] testCases) {
		for (Object[] testCase : testCases) {
            String params          =           (String)testCase[0];
            ExceptionChecker ec    = (ExceptionChecker)testCase[1];
            Class<?> expectedClass =         (Class<?>)testCase[2];

            String testMessage = getTestMessage("setters_Exceptions", params);
            exceptionNameChecker(ec, testMessage, expectedClass);
		}                                                                                                                                                  
	}
	
	public static void test_HashCode(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			Object first     =          testCase[0];
			Object second    =          testCase[1];
			boolean expected = (boolean)testCase[2];
			test_HashCodeEqualsOrNotEquals("hashCode", first, second, expected);
		}
	}
	
	public static void checkHashCodeEquals(Object first, Object second) {
		test_HashCodeEqualsOrNotEquals("hashCodeEquals", first, second, true);
	}
	
	public static void checkHashCodeNotEquals(Object first, Object second) {
		test_HashCodeEqualsOrNotEquals("hashCodeNotEquals", first, second, false);
	}
	
	private static void test_HashCodeEqualsOrNotEquals(String msg, Object first, Object second, boolean expected) {
		assertEquals( getTestMessage(msg, first, second, "expected = " + expected), expected, first.hashCode() == second.hashCode());
	}

	public static void test_NotEquals(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			Object first     =          testCase[0];
			Object second    =          testCase[1];
			test_EqualsOrNotEquals("notEquals", first, second, false);
		}
	}

	public static void test_EqualsOrNotEquals(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			Object first     =          testCase[0];
			Object second    =          testCase[1];
			boolean expected = (boolean)testCase[2];
			test_EqualsOrNotEquals("equalsOrNotEquals", first, second, expected);
		}
	}
	
	public static void test_FirstEqualsSecond_SecondNotEqualsThird(Object[][] testCases) {
		for (Object[] testCase : testCases) {
			Object orig = testCase[0];
			Object copy = testCase[1];
			Object diff = testCase[2];

			test_EqualsOrNotEquals("firstEqualsSecond",    orig, copy, true);
			test_EqualsOrNotEquals("secondNotEqualsThird", orig, diff, false);
		}
	}
	
	private static void test_EqualsOrNotEquals(String msg, Object first, Object second, boolean expected) {
		assertEquals( getTestMessage(msg, first, second, "expected = " + expected), expected, first.equals(second));
	}
}

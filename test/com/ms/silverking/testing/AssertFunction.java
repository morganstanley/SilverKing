package com.ms.silverking.testing;

import static com.ms.silverking.testing.Assert.exceptionNameChecker;
import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

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
    
    public static void test_HashCodeEquals(Object[][] testCases) {
        for (Object[] testCase : testCases) {
            Object first  = testCase[0];
            Object second = testCase[1];
            checkHashCodeEquals(first, second);
        }
    }
    
    public static void test_HashCodeNotEquals(Object[][] testCases) {
        for (Object[] testCase : testCases) {
            Object first  = testCase[0];
            Object second = testCase[1];
            checkHashCodeNotEquals(first, second);
        }
    }
    
    public static void checkHashCodeEquals(Object first, Object second) {
        test_HashCodeEqualsOrNotEquals("hashCodeEquals", first, second, true);
    }
    
    public static void checkHashCodeNotEquals(Object first, Object second) {
        test_HashCodeEqualsOrNotEquals("hashCodeNotEquals", first, second, false);
    }
    
    private static void test_HashCodeEqualsOrNotEquals(String msg, Object first, Object second, boolean expected) {
        assertEquals( getTestMessage(msg, first, second), expected, first.hashCode() == second.hashCode());
    }

    public static void test_Equals(Object[][] testCases) {
        for (Object[] testCase : testCases) {
            Object first  = testCase[0];
            Object second = testCase[1];
            test_Equals(first, second);
        }
    }

    public static void test_NotEquals(Object[][] testCases) {
        for (Object[] testCase : testCases) {
            Object first  = testCase[0];
            Object second = testCase[1];
            test_NotEquals(first, second);
        }
    }
    
    public static void test_FirstEqualsSecond_FirstNotEqualsThird(Object[][] testCases) {
        for (Object[] testCase : testCases) {
            Object first  = testCase[0];
            Object second = testCase[1];
            Object third  = testCase[2];

            test_EqualsOrNotEquals("firstEqualsSecond",   first, second, true);
            test_EqualsOrNotEquals("firstNotEqualsThird", first, third, false);
        }
    }
    
    private static void test_Equals(Object first, Object second) {
        test_EqualsOrNotEquals("Equals", first, second, true);
    }
    
    private static void test_NotEquals(Object first, Object second) {
        test_EqualsOrNotEquals("NotEquals", first, second, false);
    }
    
    private static void test_EqualsOrNotEquals(String msg, Object first, Object second, boolean expected) {
        assertEquals( getTestMessage(msg, first, second), expected, first.equals(second));
    }
}

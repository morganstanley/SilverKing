package com.ms.silverking.cloud.dht.common;

import static com.ms.silverking.testing.AssertFunction.checkHashCodeEquals;
import static com.ms.silverking.testing.AssertFunction.checkHashCodeNotEquals;
import static com.ms.silverking.testing.AssertFunction.test_Equals;
import static com.ms.silverking.testing.AssertFunction.test_NotEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class KeyAndIntegerTest {

    private static final KeyAndInteger ki         = new KeyAndInteger(0, 1, 3);
    private static final KeyAndInteger kiCopy     = new KeyAndInteger(ki, 3);
    private static final KeyAndInteger kiSemiCopy = new KeyAndInteger(1, 1, 3);
    private static final KeyAndInteger kiDiff     = new KeyAndInteger(1, 2, 3);
    
    @Test
    public void testGetters() {
        Object[][] testCases = {
            {ki,     0L, 1L, 3},
            {kiCopy, 0L, 1L, 3},
        };
        
        for (Object[] testCase : testCases) {
            KeyAndInteger ki    = (KeyAndInteger)testCase[0];
            long expectedMsl    =          (long)testCase[1];
            long expectedLsl    =          (long)testCase[2];
            int expectedInteger =           (int)testCase[3];

            assertEquals(expectedMsl,     ki.getMSL());
            assertEquals(expectedLsl,     ki.getLSL());
            assertEquals(expectedInteger, ki.getInteger());
        }
    }

    @Test
    public void testHashCode() {
        checkHashCodeEquals(   ki, ki);
        checkHashCodeEquals(   ki, kiCopy);
        checkHashCodeEquals(   ki, kiSemiCopy);
        checkHashCodeNotEquals(ki, kiDiff);
    }
    
    @Test
    public void testEqualsObject() {
        test_Equals(new KeyAndInteger[][]{
            {ki,     ki},
            {kiCopy, ki},
        });
        
        test_NotEquals(new KeyAndInteger[][]{
            {ki, kiSemiCopy},
            {ki, kiDiff},
        });
    }
    
    @Test
    public void testToString() {
        Object[][] testCases = {
            {ki,         "0:1:3"},
            {kiCopy,     "0:1:3"},
            {kiSemiCopy, "1:1:3"},
            {kiDiff,     "1:2:3"},
        };
        
        for (Object[] testCase : testCases) {
            KeyAndInteger ki      = (KeyAndInteger)testCase[0];
            String expectedString =        (String)testCase[1];

            assertEquals(expectedString, ki.toString());
        }
    }
}

package com.ms.silverking.testing;

import org.junit.Test;

import com.ms.silverking.util.test.FieldAccessor;
import static org.junit.Assert.assertEquals;

public class TestAccess {
    private static final String      stringValue = "a";
    private static final byte        byteValue = (byte)1;
    private static final short       shortValue = (short)2;
    private static final int         intValue = (int)3;
    private static final long        longValue = (long)4;
    private static final float       floatValue = (float)5;
    private static final double      doubleValue = (double)6;
    private static final boolean     booleanValue = true;
    private static final char        charValue = 'b';
 
    @Test
    public void test() {
        try {
            TestFieldAccessSubject t;
               
            t = new TestFieldAccessSubject();
               
            FieldAccessor.set(t, TestFieldAccessSubject.stringFieldName, stringValue);
            FieldAccessor.set(t, TestFieldAccessSubject.byteFieldName, byteValue);
            FieldAccessor.set(t, TestFieldAccessSubject.shortFieldName, shortValue);
            FieldAccessor.set(t, TestFieldAccessSubject.intFieldName, intValue);
            FieldAccessor.set(t, TestFieldAccessSubject.longFieldName, longValue);
            FieldAccessor.set(t, TestFieldAccessSubject.floatFieldName, floatValue);
            FieldAccessor.set(t, TestFieldAccessSubject.doubleFieldName, doubleValue);
            FieldAccessor.set(t, TestFieldAccessSubject.charFieldName, charValue);
            FieldAccessor.set(t, TestFieldAccessSubject.booleanFieldName, booleanValue);
               
            assertEquals(TestFieldAccessSubject.stringFieldName, FieldAccessor.getObject(t, TestFieldAccessSubject.stringFieldName), stringValue);
            assertEquals(TestFieldAccessSubject.byteFieldName, FieldAccessor.getByte(t, TestFieldAccessSubject.byteFieldName), byteValue); 
            assertEquals(TestFieldAccessSubject.shortFieldName, FieldAccessor.getShort(t, TestFieldAccessSubject.shortFieldName), shortValue); 
            assertEquals(TestFieldAccessSubject.intFieldName, FieldAccessor.getInt(t, TestFieldAccessSubject.intFieldName), intValue); 
            assertEquals(TestFieldAccessSubject.longFieldName, FieldAccessor.getLong(t, TestFieldAccessSubject.longFieldName), longValue); 
            assertEquals(TestFieldAccessSubject.floatFieldName, FieldAccessor.getFloat(t, TestFieldAccessSubject.floatFieldName), floatValue, 0.0f); 
            assertEquals(TestFieldAccessSubject.doubleFieldName, FieldAccessor.getDouble(t, TestFieldAccessSubject.doubleFieldName), doubleValue, 0.0); 
            assertEquals(TestFieldAccessSubject.charFieldName, FieldAccessor.getChar(t, TestFieldAccessSubject.charFieldName), charValue); 
            assertEquals(TestFieldAccessSubject.booleanFieldName, FieldAccessor.getBoolean(t, TestFieldAccessSubject.booleanFieldName), booleanValue); 
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
     
    public static void main(String[] args) {
        new TestAccess().test();
    }
}

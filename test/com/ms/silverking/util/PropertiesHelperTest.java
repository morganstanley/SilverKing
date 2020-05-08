package com.ms.silverking.util;

import static com.ms.silverking.testing.Assert.exceptionNameChecker;
import static com.ms.silverking.testing.Util.getTestMessage;
import static com.ms.silverking.util.PropertiesHelper.ParseExceptionAction.DefaultOnParseException;
import static com.ms.silverking.util.PropertiesHelper.ParseExceptionAction.RethrowParseException;
import static com.ms.silverking.util.PropertiesHelper.UndefinedAction.DefaultOnUndefined;
import static com.ms.silverking.util.PropertiesHelper.UndefinedAction.ExceptionOnUndefined;
import static com.ms.silverking.util.PropertiesHelper.UndefinedAction.ZeroOnUndefined;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.testing.Util.ExceptionChecker;
import com.ms.silverking.util.PropertiesHelper.ParseExceptionAction;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

public class PropertiesHelperTest {

    private PropertiesHelper ph;
    private Properties properties;
    
    private static final String key1        = "k1";
    private static final String value1      = "v1";
    private static final String key2        = "k2";
    private static final String value2      = "v2";
    private static final String keyInt1     = "1";
    private static final String valueInt1   = "1";
    private static final String keyInt2     = "2";
    private static final String valueIntTwo = "two";
    private static final String keyBool     = "true";
    private static final String valueBool   = "true";
    private static final String keyLong     = "1000L";
    private static final String valueLong   = "1000";
    private static final String keyA        = "a";
    private static final String keyEnum     = "enumProperty";
    private static final String valueEnum   = "LZ4";
    
    @Before
    public void setUp() throws Exception {
        properties = new Properties();
        setProperty(key1,    value1);
        setProperty(key2,    value2);
        setProperty(keyInt1, valueInt1);
        setProperty(keyInt2, valueIntTwo);
        setProperty(keyBool, valueBool);
        setProperty(keyLong, valueLong);
        setProperty(keyEnum, valueEnum);
        ph = new PropertiesHelper(properties);
    }
    
    private void setProperty(String k, String v) {
        properties.setProperty(k, v);
    }

    @Test
    public void testGetStringAllParams_Exceptions() {
        Object[][] testCases = {
            {null, null,                 null, NullPointerException.class},
            {keyA, null,                 null, NullPointerException.class},
            {keyA, null, ExceptionOnUndefined,    PropertyException.class},
        };
        
        for (Object[] testCase : testCases) {
            String name                     =          (String)testCase[0];
            String defaultValue             =          (String)testCase[1];
            UndefinedAction undefinedAction = (UndefinedAction)testCase[2];
            Class<?> expectedExceptionClass =        (Class<?>)testCase[3];
            
            String testMessage = getTestMessage("getString_Exceptions", name, defaultValue, undefinedAction, null); 
            ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkGetStringAllParams(name, defaultValue, undefinedAction, null); } };
            exceptionNameChecker(ec, testMessage, expectedExceptionClass);
        }
    }
    
    @Test
    public void testGetStringAllParams() {
        Object[][] testCases = {
            {key1, null,               null, value1},
            {keyA, null,    ZeroOnUndefined,   null},
            {keyA, null, DefaultOnUndefined,   null},
            {keyA,  "b", DefaultOnUndefined,    "b"},
        };
        
        for (Object[] testCase : testCases) {
            String name                     =          (String)testCase[0];
            String defaultValue             =          (String)testCase[1];
            UndefinedAction undefinedAction = (UndefinedAction)testCase[2];
            String expected                 =          (String)testCase[3];
            
            checkGetStringAllParams(name, defaultValue, undefinedAction, expected);
        }
    }
    
    private void checkGetStringAllParams(String name, String defaultValue, UndefinedAction undefinedAction, String expected) {
        assertEquals( getTestMessage("getString", name,    defaultValue, undefinedAction), expected, ph.getString(name, defaultValue, undefinedAction));
    }
    
    @Test
    public void testGetIntAllParams_Exceptions() {
        Object[][] testCases = {
            {null,    -1,                 null,                  null,  NullPointerException.class},
            {keyA,    -1,                 null,                  null,  NullPointerException.class},
            {keyA,    -1, ExceptionOnUndefined,                  null,     PropertyException.class},
            {keyInt2, -1,                 null, RethrowParseException, NumberFormatException.class},
        };
        
        for (Object[] testCase : testCases) {
            String name                               =               (String)testCase[0];
            int defaultValue                          =                  (int)testCase[1];
            UndefinedAction undefinedAction           =      (UndefinedAction)testCase[2];
            ParseExceptionAction parseExceptionAction = (ParseExceptionAction)testCase[3];
            Class<?> expectedExceptionClass           =             (Class<?>)testCase[4];
            
            String testMessage = getTestMessage("getInt_Exceptions", name, defaultValue, undefinedAction, parseExceptionAction); 
            ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkGetIntAllParams(name, defaultValue, undefinedAction, parseExceptionAction, -1); } };
            exceptionNameChecker(ec, testMessage, expectedExceptionClass);
        }
    }
    
    @Test
    public void testGetIntAllParams() {
        Object[][] testCases = {
            {keyInt1, -1,               null,                    null, Integer.parseInt( valueInt1 )},
            {keyInt2, -1,               null, DefaultOnParseException,                               -1},
            {keyA,    -1,    ZeroOnUndefined,                    null,                             0},
            {keyA,    -1, DefaultOnUndefined,                    null,                               -1},
        };
        
        for (Object[] testCase : testCases) {
            String name                               =               (String)testCase[0];
            int defaultValue                          =                  (int)testCase[1];
            UndefinedAction undefinedAction           =      (UndefinedAction)testCase[2];
            ParseExceptionAction parseExceptionAction = (ParseExceptionAction)testCase[3];
            int expected                              =                  (int)testCase[4];
            
            checkGetIntAllParams(name, defaultValue, undefinedAction, parseExceptionAction, expected);
        }
    }
    
    private void checkGetIntAllParams(String name, int defaultValue, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction, int expected) {
        assertEquals( getTestMessage("getInt", name, defaultValue, undefinedAction, parseExceptionAction),
                expected, ph.getInt(name, defaultValue, undefinedAction, parseExceptionAction));
    }

    @Test
    public void testGetBooleanAllParams_Exceptions() {
        Object[][] testCases = {
            {null,    true,                 null,                  null, NullPointerException.class},
            {keyA,    true,                 null,                  null, NullPointerException.class},
            {keyA,    true, ExceptionOnUndefined,                  null,    PropertyException.class},
            {keyInt1, true,                 null, RethrowParseException,     RuntimeException.class},
        };
        
        for (Object[] testCase : testCases) {
            String name                               =               (String)testCase[0];
            boolean defaultValue                      =              (boolean)testCase[1];
            UndefinedAction undefinedAction           =      (UndefinedAction)testCase[2];
            ParseExceptionAction parseExceptionAction = (ParseExceptionAction)testCase[3];
            Class<?> expectedExceptionClass           =             (Class<?>)testCase[4];
            
            String testMessage = getTestMessage("getBoolan_Exceptions", name, defaultValue, undefinedAction, parseExceptionAction); 
            ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkGetBooleanAllParams(name, defaultValue, undefinedAction, parseExceptionAction, false); } };
            exceptionNameChecker(ec, testMessage, expectedExceptionClass);
        }
    }
    
    @Test
    public void testGetBooleanAllParams() {
        Object[][] testCases = {
            {keyBool, true,               null,                    null, Boolean.parseBoolean(valueBool)},
            {keyInt1, true,               null, DefaultOnParseException,                             true},
            {keyA,    true,    ZeroOnUndefined,                    null,                             false},
            {keyA,    true, DefaultOnUndefined,                    null,                             true},
        };
        
        for (Object[] testCase : testCases) {
            String name                               =               (String)testCase[0];
            boolean defaultValue                      =              (boolean)testCase[1];
            UndefinedAction undefinedAction           =      (UndefinedAction)testCase[2];
            ParseExceptionAction parseExceptionAction = (ParseExceptionAction)testCase[3];
            boolean expected                          =              (boolean)testCase[4];
            
            checkGetBooleanAllParams(name, defaultValue, undefinedAction, parseExceptionAction, expected);
        }
    }
    
    private void checkGetBooleanAllParams(String name, boolean defaultValue, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction, boolean expected) {
        assertEquals( getTestMessage("getBoolean", name, defaultValue, undefinedAction, parseExceptionAction),
                expected, ph.getBoolean(name, defaultValue, undefinedAction, parseExceptionAction));
    }

    @Test
    public void testGetLongAllParams_Exceptions() {
        Object[][] testCases = {
            {null,    -1L,                 null,                  null,  NullPointerException.class},
            {keyA,    -1L,                 null,                  null,  NullPointerException.class},
            {keyA,    -1L, ExceptionOnUndefined,                  null,     PropertyException.class},
            {keyInt2, -1L,                 null, RethrowParseException, NumberFormatException.class},
        };
        
        for (Object[] testCase : testCases) {
            String name                               =               (String)testCase[0];
            long defaultValue                         =                 (long)testCase[1];
            UndefinedAction undefinedAction           =      (UndefinedAction)testCase[2];
            ParseExceptionAction parseExceptionAction = (ParseExceptionAction)testCase[3];
            Class<?> expectedExceptionClass           =             (Class<?>)testCase[4];
            
            String testMessage = getTestMessage("getLong_Exceptions", name, defaultValue, undefinedAction, parseExceptionAction); 
            ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkGetLongAllParams(name, defaultValue, undefinedAction, parseExceptionAction, -1L); } };
            exceptionNameChecker(ec, testMessage, expectedExceptionClass);
        }
    }
    
    @Test
    public void testGetLongAllParams() {
        Object[][] testCases = {
            {keyLong, -1L,               null,                    null, Long.parseLong( valueLong )},
            {keyInt2, -1L,               null, DefaultOnParseException,                         -1L},
            {keyA,    -1L,    ZeroOnUndefined,                    null,                           0L},
            {keyA,    -1L, DefaultOnUndefined,                    null,                         -1L},
        };
        
        for (Object[] testCase : testCases) {
            String name                               =               (String)testCase[0];
            long defaultValue                      =                    (long)testCase[1];
            UndefinedAction undefinedAction           =      (UndefinedAction)testCase[2];
            ParseExceptionAction parseExceptionAction = (ParseExceptionAction)testCase[3];
            long expected                          =                    (long)testCase[4];
            
            checkGetLongAllParams(name, defaultValue, undefinedAction, parseExceptionAction, expected);
        }
    }
    
    private void checkGetLongAllParams(String name, long defaultValue, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction, long expected) {
        assertEquals( getTestMessage("getLong", name, defaultValue, undefinedAction, parseExceptionAction),
                expected, ph.getLong(name, defaultValue, undefinedAction, parseExceptionAction));
    }

    public void testGetEnumAllParams_Exceptions() {
        Object[][] testCases = {
            {null,    Compression.BZIP2,                 null,                  null,  NullPointerException.class},
            {keyEnum,    Compression.BZIP2,                 null,                  null,  NullPointerException.class},
            {keyEnum,    Compression.BZIP2, ExceptionOnUndefined,                  null,     PropertyException.class},
            {keyEnum, Compression.BZIP2,                 null, RethrowParseException, NumberFormatException.class},
        };
        
        for (Object[] testCase : testCases) {
            String name                               =               (String)testCase[0];
            Compression defaultValue                         =                 (Compression)testCase[1];
            UndefinedAction undefinedAction           =      (UndefinedAction)testCase[2];
            ParseExceptionAction parseExceptionAction = (ParseExceptionAction)testCase[3];
            Class<?> expectedExceptionClass           =             (Class<?>)testCase[4];
            
            String testMessage = getTestMessage("getLong_Exceptions", name, defaultValue, undefinedAction, parseExceptionAction); 
            ExceptionChecker ec = new ExceptionChecker() { @Override public void check(){ checkGetEnumAllParams(name, defaultValue, undefinedAction, parseExceptionAction, Compression.BZIP2); } };
            exceptionNameChecker(ec, testMessage, expectedExceptionClass);
        }
    }
    
    @Test
    public void testGetEnumAllParams() {
        Object[][] testCases = {
            {keyEnum, Compression.BZIP2,               null,                    null, Compression.valueOf( valueEnum )},
            {"nokey", Compression.BZIP2,               null, DefaultOnParseException,                         Compression.BZIP2},
            {"nokey",    Compression.BZIP2,    ZeroOnUndefined,                    null,                           null},
            {"nokey",    Compression.BZIP2, DefaultOnUndefined,                    null,                         Compression.BZIP2},
        };
        
        for (Object[] testCase : testCases) {
            String name                               =               (String)testCase[0];
            Compression defaultValue                      =                    (Compression)testCase[1];
            UndefinedAction undefinedAction           =      (UndefinedAction)testCase[2];
            ParseExceptionAction parseExceptionAction = (ParseExceptionAction)testCase[3];
            Compression expected                          =                    (Compression)testCase[4];
            
            checkGetEnumAllParams(name, defaultValue, undefinedAction, parseExceptionAction, expected);
        }
    }
    
    private void checkGetEnumAllParams(String name, Compression defaultValue, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction, Compression expected) {
        //System.out.printf("%s %s %s %s %s\n", name, defaultValue, undefinedAction, parseExceptionAction, expected);
        assertEquals( getTestMessage("getEnum", name, defaultValue, undefinedAction, parseExceptionAction),
                expected, ph.getEnum(name, defaultValue, undefinedAction, parseExceptionAction));
    }
}

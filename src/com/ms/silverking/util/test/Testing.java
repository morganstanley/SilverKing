package com.ms.silverking.util.test;

import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;

public class Testing {
    private static final boolean    testingEnabled;
    
    public static final String    testingEnabledProperty = Testing.class.getCanonicalName() +".testingEnabled";
    private static final boolean  testingEnabledDefault = false;
    
    static {
        testingEnabled = PropertiesHelper.systemHelper.getBoolean(testingEnabledProperty, testingEnabledDefault);
        if (testingEnabled) {
            Log.warning(testingEnabledProperty +" is true");
        } else {
            Log.fine(testingEnabledProperty +" is false");
        }
    }
    
    public static final boolean getTestingEnabled() {
        return testingEnabled;
    }
    
    public static final void ensureTestingEnabled() {
        if (!getTestingEnabled()) {
            throw new RuntimeException("Testing.ensureTestingEnabled() failed");
        }
    }
}

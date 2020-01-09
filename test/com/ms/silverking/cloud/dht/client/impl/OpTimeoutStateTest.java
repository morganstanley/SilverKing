package com.ms.silverking.cloud.dht.client.impl;

import static com.ms.silverking.testing.Util.getTestMessage;
import static com.ms.silverking.testing.Util.int_maxVal;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutControllerTest;
import com.ms.silverking.cloud.dht.client.SimpleTimeoutControllerTest;
import com.ms.silverking.cloud.dht.client.WaitForTimeoutControllerTest;

public class OpTimeoutStateTest {
    
    private final OpTimeoutState opSizeBasedTimeoutState;
    private final OpTimeoutState simpleTimeoutState;
    private final OpTimeoutState waitForTimeoutState;

    private static final int opSizeBasedTimeoutController_defaultNonKeyedOpMaxRelTimeout_ms              = 25 * 60 * 1000;
    private static final int opSizeBasedTimeoutController_defaultMaxAttempts                             = 4;
    private static final int opSizeBasedTimeoutController_defaultExclusionChangeRetryIntervalMS          = 5 * 1000;
    
    private static final int simpleTimeoutController_getRelativeTimeoutMillisForAttempt                  = 2 * 60 * 1000;
    private static final int simpleTimeoutController_defaultMaxRelativeTimeoutMillis                     = 2 * 60 * 1000;
    private static final int simpleTimeoutController_defaultMaxAttempts                                  = 5;
    private static final int simpleTimeoutController_getRelativeExclusionChangeRetryMillisForAttempt     = 2 * 60 * 1000;
    
    private static final int waitForTimeoutController_defaultInternalRetryIntervalSeconds                = 20 * 1000;
    private static final int waitForTimeoutController_defaultMaxAttempts                                 = int_maxVal;
    private static final int waitForTimeoutController_defaultExclusionChangeInternalRetryIntervalSeconds = 2 * 1000;
    
    private static final long zeroTimeMillis = 0;
    
    public OpTimeoutStateTest() {
        opSizeBasedTimeoutState = new OpTimeoutState(null, OpSizeBasedTimeoutControllerTest.defaultController, zeroTimeMillis);
        simpleTimeoutState      = new OpTimeoutState(null,      SimpleTimeoutControllerTest.defaultController, zeroTimeMillis);
        waitForTimeoutState     = new OpTimeoutState(null,     WaitForTimeoutControllerTest.defaultController, zeroTimeMillis);
    }
    
    @Test
    public void testGetCurRelTimeoutMillis() {
        Object[][] testCases = {
            {opSizeBasedTimeoutController_defaultNonKeyedOpMaxRelTimeout_ms, opSizeBasedTimeoutState},
            //{simpleTimeoutController_getRelativeTimeoutMillisForAttempt,          simpleTimeoutState},
            {waitForTimeoutController_defaultInternalRetryIntervalSeconds,       waitForTimeoutState},
        };

        for (Object[] testCase : testCases) {
            int expected         =            (int)testCase[0];
            OpTimeoutState state = (OpTimeoutState)testCase[1];
            
            assertEquals( getTestMessage("opHasTimedOut", state), expected, state.getCurRelTimeoutMillis());
        }
    }
    
    // FUTURE:bph: comments
    
    @Test
    public void testOpHasTimedOut() {
        Object[][] testCases = {
            {opSizeBasedTimeoutState, zeroTimeMillis,                                                         false},
            {opSizeBasedTimeoutState, (long)opSizeBasedTimeoutController_defaultNonKeyedOpMaxRelTimeout_ms,   false},
            {opSizeBasedTimeoutState, (long)opSizeBasedTimeoutController_defaultNonKeyedOpMaxRelTimeout_ms+1,  true},
            {simpleTimeoutState,      zeroTimeMillis,                                                         false},
            {simpleTimeoutState,      (long)simpleTimeoutController_defaultMaxRelativeTimeoutMillis,          false},
            {simpleTimeoutState,      (long)simpleTimeoutController_defaultMaxRelativeTimeoutMillis+1,         true},
//            {waitForTimeoutState,     zeroTimeMillis,                                                         false},    // NPE if AsyncOperation param is null, testing with null b/c it's too much work to create an actual AsyncOperation...
//            {waitForTimeoutState,     (long)fixme,          false}, // NPE if AsyncOperation param is null, testing with null b/c it's too much work to create an actual AsyncOperation...
//            {waitForTimeoutState,     (long)fixme+1,         true}, // NPE if AsyncOperation param is null, testing with null b/c it's too much work to create an actual AsyncOperation...
        };

        for (Object[] testCase : testCases) {
            OpTimeoutState state = (OpTimeoutState)testCase[0];
            long curTimeMillis   =           (long)testCase[1];
            boolean expected     =        (boolean)testCase[2];

            checkOpHasTimedOut(expected, state, curTimeMillis);
        }
        
        Object[][] testCases_curAttemptIndex = {
            {opSizeBasedTimeoutState, opSizeBasedTimeoutController_defaultMaxAttempts, true},
            {simpleTimeoutState,           simpleTimeoutController_defaultMaxAttempts, true},
            {waitForTimeoutState,         waitForTimeoutController_defaultMaxAttempts, true},    // works b/c the || in the conditional short-circuits, so we don't hit the NPE
        };

        for (Object[] testCase : testCases_curAttemptIndex) {
            OpTimeoutState state = (OpTimeoutState)testCase[0];
            int maxAttempts      =            (int)testCase[1];
            boolean expected     =        (boolean)testCase[2];

            // fast-forward to maxAttempts
            for (int i = 0; i < maxAttempts; i++)
                state.newAttempt(zeroTimeMillis);
            
            checkOpHasTimedOut(expected, state, zeroTimeMillis);
        }
    }
    
    private void checkOpHasTimedOut(boolean expected, OpTimeoutState state, long curTimeMillis) {
        assertEquals( getTestMessage("opHasTimedOut", state, curTimeMillis), expected, state.opHasTimedOut(curTimeMillis));
    }
    
    @Test
    public void testAttemptHasTimedOut() {
        Object[][] testCases = {
            {opSizeBasedTimeoutState, zeroTimeMillis,                                                         false},
            {opSizeBasedTimeoutState, (long)opSizeBasedTimeoutController_defaultNonKeyedOpMaxRelTimeout_ms,   false},
            {opSizeBasedTimeoutState, (long)opSizeBasedTimeoutController_defaultNonKeyedOpMaxRelTimeout_ms+1,  true},
            {simpleTimeoutState,      zeroTimeMillis,                                                         false},
            {simpleTimeoutState,      (long)simpleTimeoutController_getRelativeTimeoutMillisForAttempt,       false},
            {simpleTimeoutState,      (long)simpleTimeoutController_getRelativeTimeoutMillisForAttempt+1,      true},
            {waitForTimeoutState,     zeroTimeMillis,                                                         false},
            {waitForTimeoutState,     (long)waitForTimeoutController_defaultInternalRetryIntervalSeconds,     false},
            {waitForTimeoutState,     (long)waitForTimeoutController_defaultInternalRetryIntervalSeconds+1,    true},
        };

        for (Object[] testCase : testCases) {
            OpTimeoutState state = (OpTimeoutState)testCase[0];
            long curTimeMillis   =           (long)testCase[1];
            boolean expected     =        (boolean)testCase[2];

            assertEquals( getTestMessage("attemptHasTimedOut", state, curTimeMillis), expected, state.attemptHasTimedOut(curTimeMillis));
        }
    }
    
    @Test
    public void testRetryOnExclusionChange() {
        Object[][] testCases = {
            {opSizeBasedTimeoutState, zeroTimeMillis,                                                                      false},
            {opSizeBasedTimeoutState, (long)opSizeBasedTimeoutController_defaultExclusionChangeRetryIntervalMS,            false},
            {opSizeBasedTimeoutState, (long)opSizeBasedTimeoutController_defaultExclusionChangeRetryIntervalMS+1,           true},
            {simpleTimeoutState,      zeroTimeMillis,                                                                      false},
            {simpleTimeoutState,      (long)simpleTimeoutController_getRelativeExclusionChangeRetryMillisForAttempt,       false},
            {simpleTimeoutState,      (long)simpleTimeoutController_getRelativeExclusionChangeRetryMillisForAttempt+1,      true},
            {waitForTimeoutState,     zeroTimeMillis,                                                                      false},
            {waitForTimeoutState,     (long)waitForTimeoutController_defaultExclusionChangeInternalRetryIntervalSeconds,   false},
            {waitForTimeoutState,     (long)waitForTimeoutController_defaultExclusionChangeInternalRetryIntervalSeconds+1,  true},
        };

        for (Object[] testCase : testCases) {
            OpTimeoutState state = (OpTimeoutState)testCase[0];
            long curTimeMillis   =           (long)testCase[1];
            boolean expected     =        (boolean)testCase[2];

            assertEquals( getTestMessage("retryOnExclusionChange", state, curTimeMillis), expected, state.retryOnExclusionChange(curTimeMillis));
        }
    }
    
    // being tested in testOpHasTimedOut
//    @Test
//    public void testNewAttempt() {
//    }
    
    @Test
    public void testToString() {
        Object[][] testCases = {
            {"0:0:maxAttempts=4,constantTime_ms=300000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1500000,exclusionChangeRetryInterval_ms=5000", opSizeBasedTimeoutState},
            {"0:0:maxAttempts=5,maxRelativeTimeoutMillis=120000",                                                                                     simpleTimeoutState},
            {"0:0:internalRetryIntervalSeconds=20,internalExclusionChangeRetryIntervalSeconds=2",                                                    waitForTimeoutState},
        };

        for (Object[] testCase : testCases) {
            String expected      =         (String)testCase[0];
            OpTimeoutState state = (OpTimeoutState)testCase[1];

            assertEquals(expected, state.toString());
        }
    }

}

package com.ms.silverking.net.security;

import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AuthenticatorSKReflectionTest {
    
    @Test
    public void testToSkDefAndParse_successful_getAuthId_getFailedAction() {
        Object[][] testCases = {
            {new DummyAuthenticatorImpl1("MyDummy1", 1999), true,  DummyAuthenticatorImpl1.dummyAuthId, AuthFailedAction.THROW_ERROR},
            {new DummyAuthenticatorImpl2(),                 false, null,                                DummyAuthenticatorImpl2.failedAction},
            {new NoopAuthenticatorImpl(),                   false, null,                                AuthFailedAction.GO_WITHOUT_AUTH},
        };
        
        for (Object[] testCase : testCases) {
            Authenticator authenticator     =    (Authenticator)testCase[0];
            boolean expectedSuccess         =          (boolean)testCase[1];
            String expectedId               =           (String)testCase[2];
            AuthFailedAction expectedAction = (AuthFailedAction)testCase[3];
            
            String def = authenticator.toSKDef();
            Authenticator parsedAuthenticator = Authenticator.parseSKDef(def);
            
//            checkSKDefAndParse(dummyAuthenticator);   // Authenticator needs to override equals for this to work, will just compare names for now instead
            assertEquals(authenticator.getName(), parsedAuthenticator.getName());
            
            AuthResult result = parsedAuthenticator.syncAuthenticate(null, true, 0);

            assertEquals(getTestMessage("isSuccessful",         "impl=[" + authenticator.getClass().getCanonicalName() + "]"), expectedSuccess, result.isSuccessful());
            if (expectedId != null)
                assertEquals(getTestMessage("getAuthId",        "impl=[" + authenticator.getClass().getCanonicalName() + "]"), expectedId,      result.getAuthId().get());
            assertEquals(getTestMessage("createAuthFailResult", "impl=[" + authenticator.getClass().getCanonicalName() + "]"), expectedAction,  result.getFailedAction());
        }
    }
    
    private void checkSKDefAndParse(Authenticator authenticator) {
        assertEquals(getTestMessage("checkSKDefAndParse", "SKStringDef = [" + authenticator.toSKDef() + "]"), authenticator, Authenticator.parseSKDef( authenticator.toSKDef() ));
    }
}

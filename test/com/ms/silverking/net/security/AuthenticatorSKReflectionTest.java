package com.ms.silverking.net.security;

import org.junit.Test;

import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuthenticatorSKReflectionTest {
    @Test
    public void testSKReflectionImplWithArgs() {
        String strArg = "MyDummpy1";
        int intArg = 1999;
        DummyAuthenticatorImpl1 authImpl = new DummyAuthenticatorImpl1(strArg, intArg);
        String def = authImpl.toSKDef();
        Authenticator auth = Authenticator.parseSKDef(def);
        String actualName = auth.getName();
        String expectedName1 = authImpl.getName();
        String expectedName2 = DummyAuthenticatorImpl1.constructName(strArg, intArg);

        assertEquals(getTestMessage("Authenticator.toSKDef/parseSKDef", "SKStringDef = [" + def + "]"),
                expectedName1, actualName);
        assertEquals(getTestMessage("Authenticator.toSKDef/parseSKDef", "SKStringDef = [" + def + "]"),
                expectedName2, actualName);

        Authenticator.AuthResult res = auth.syncAuthenticate(null, true, 0);
        assertTrue(getTestMessage("Authenticator.createAuthSuccessResult", "impl=[" + DummyAuthenticatorImpl1.class.getCanonicalName() + "]"),
                res.getAuthId().isPresent());
        assertEquals(getTestMessage("Authenticator.createAuthSuccessResult", "impl=[" + DummyAuthenticatorImpl1.class.getCanonicalName() + "]"),
                res.getAuthId().get(), DummyAuthenticatorImpl1.dummyAuthId);
    }

    @Test
    public void testSKReflectionImplWithNoArgs() {
        DummyAuthenticatorImpl2 authImpl = new DummyAuthenticatorImpl2();
        String def = authImpl.toSKDef();
        Authenticator auth = Authenticator.parseSKDef(def);
        String actualName = auth.getName();
        String expectedName1 = authImpl.getName();
        String expectedName2 = DummyAuthenticatorImpl2.constructName();

        assertEquals(getTestMessage("Authenticator.toSKDef/parseSKDef", "SKStringDef = [" + def + "]"),
                expectedName1, actualName);
        assertEquals(getTestMessage("Authenticator.toSKDef/parseSKDef", "SKStringDef = [" + def + "]"),
                expectedName2, actualName);

        Authenticator.AuthResult res = auth.syncAuthenticate(null, true, 0);
        assertFalse(getTestMessage("Authenticator.createAuthFailResult", "impl=[" + DummyAuthenticatorImpl2.class.getCanonicalName() + "]"),
                res.getAuthId().isPresent());
        assertEquals(getTestMessage("Authenticator.createAuthFailResult", "impl=[" + DummyAuthenticatorImpl2.class.getCanonicalName() + "]"),
                res.getFailedAction(), DummyAuthenticatorImpl2.failedAction);
    }

    @Test
    public void testNoopImpl() {
        NoopAuthenticatorImpl authImpl = new NoopAuthenticatorImpl();
        String def = authImpl.toSKDef();
        Authenticator auth = Authenticator.parseSKDef(def);

        assertEquals(getTestMessage("Authenticator.toSKDef/parseSKDef", "SKStringDef = [" + def + "]"),
                auth.getName(), authImpl.getName());

        Authenticator.AuthResult res1 = auth.syncAuthenticate(null, true, 0);
        Authenticator.AuthResult res2 = authImpl.syncAuthenticate(null, true, 0);
        assertEquals(getTestMessage("Authenticator.syncAuthenticate", "impl=[" + NoopAuthenticatorImpl.class.getCanonicalName() + "]"),
                res1.isSuccessful(), res2.isSuccessful());
        assertEquals(getTestMessage("Authenticator.syncAuthenticate", "impl=[" + NoopAuthenticatorImpl.class.getCanonicalName() + "]"),
                res1.getFailedAction(), res2.getFailedAction());
    }
}

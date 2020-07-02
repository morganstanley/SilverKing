package com.ms.silverking.net.security;

import java.net.Socket;

import com.ms.silverking.text.ObjectDefParser2;

public class DummyAuthenticatorImpl2 extends Authenticator {
    static {
        ObjectDefParser2.addParser(new DummyAuthenticatorImpl2());
    }

    static String constructName() {
        return DummyAuthenticatorImpl2.class.getCanonicalName() + "()";
    }
    final static AuthFailedAction failedAction = AuthFailedAction.GO_WITHOUT_AUTH;

    public DummyAuthenticatorImpl2() {
    }

    @Override
    public String getName() {
        return constructName();
    }

    @Override
    public Authenticator createLocalCopy() {
        return this;
    }

    @Override
    public AuthFailedAction onAuthTimeout(boolean serverside) {
        return AuthFailedAction.GO_WITHOUT_AUTH;
    }

    @Override
    public AuthResult syncAuthenticate(Socket unauthNetwork, boolean serverside, int timeoutInMillisecond) {
        return Authenticator.createAuthFailResult(failedAction);
    }
}

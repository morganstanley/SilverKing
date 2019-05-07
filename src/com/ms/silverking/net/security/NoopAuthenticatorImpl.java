package com.ms.silverking.net.security;

import java.net.Socket;

public class NoopAuthenticatorImpl implements Authenticator {
    @Override
    public String getName() { return "[SilverkingDefaultAuthenticator]" + NoopAuthenticatorImpl.class.getCanonicalName(); }

    @Override
    public AuthFailedAction onAuthTimeout(boolean serverside) {
        return AuthFailedAction.GO_WITHOUT_AUTH;
    }

    @Override
    public AuthResult syncAuthenticate(Socket unauthNetwork, boolean serverside, int timeoutInMillisecond) {
        return Authenticator.createAuthFailResult(AuthFailedAction.GO_WITHOUT_AUTH);
    }
}

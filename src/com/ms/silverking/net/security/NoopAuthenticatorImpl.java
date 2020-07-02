package com.ms.silverking.net.security;

import com.ms.silverking.text.ObjectDefParser2;

import java.net.Socket;

public class NoopAuthenticatorImpl extends Authenticator {
  static {
    ObjectDefParser2.addParser(new NoopAuthenticatorImpl());
  }

  @Override
  public String getName() {
    return "[SilverKingDefaultAuthenticator]" + NoopAuthenticatorImpl.class.getCanonicalName();
  }

  @Override
  public Authenticator createLocalCopy() {
    // NoopAuthenticatorImpl instance is safe to be shared among threads
    return this;
  }

  @Override
  public AuthFailedAction onAuthTimeout(boolean serverside) {
    return AuthFailedAction.GO_WITHOUT_AUTH;
  }

  @Override
  public AuthResult syncAuthenticate(Socket unauthNetwork, boolean serverside, int timeoutInMillisecond) {
    return Authenticator.createAuthFailResult(AuthFailedAction.GO_WITHOUT_AUTH);
  }
}

package com.ms.silverking.net.security;

import java.net.Socket;

import com.ms.silverking.text.ObjectDefParser2;

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
  public AuthenticationFailedAction onAuthTimeout(boolean serverside) {
    return AuthenticationFailedAction.GO_WITHOUT_AUTH;
  }

  @Override
  public AuthenticationResult syncAuthenticate(Socket unauthNetwork, boolean serverside, int timeoutInMillisecond) {
    return Authenticator.createAuthFailResult(AuthenticationFailedAction.GO_WITHOUT_AUTH);
  }
}

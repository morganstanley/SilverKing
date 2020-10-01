package com.ms.silverking.net.security;

import java.net.Socket;

import com.ms.silverking.text.ObjectDefParser2;

public class DummyAuthenticatorImpl1 extends Authenticator {

  static {
    ObjectDefParser2.addParser(new DummyAuthenticatorImpl1("", 0));
  }

  private String strMember;
  private int intMember;

  static String constructName(String strMember, int intMember) {
    return DummyAuthenticatorImpl1.class.getCanonicalName() + "(" + strMember + "," + intMember + ")";
  }

  final static String dummyAuthId = "dummy1";

  public DummyAuthenticatorImpl1(String arg1, int arg2) {
    this.strMember = arg1;
    this.intMember = arg2;
  }

  @Override
  public String getName() {
    return constructName(strMember, intMember);
  }

  @Override
  public Authenticator createLocalCopy() {
    return new DummyAuthenticatorImpl1(strMember, intMember);
  }

  @Override
  public AuthenticationFailedAction onAuthTimeout(boolean serverside) {
    return AuthenticationFailedAction.GO_WITHOUT_AUTH;
  }

  @Override
  public AuthenticationResult syncAuthenticate(Socket unauthNetwork, boolean serverside, int timeoutInMillisecond) {
    return Authenticator.createAuthSuccessResult(dummyAuthId);
  }
}

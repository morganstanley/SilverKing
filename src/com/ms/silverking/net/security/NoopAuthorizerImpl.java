package com.ms.silverking.net.security;

import java.util.Optional;

import com.ms.silverking.text.ObjectDefParser2;

public class NoopAuthorizerImpl extends Authorizer {

  static {
    ObjectDefParser2.addParser(new NoopAuthorizerImpl());
  }

  @Override
  public String getName() {
    return "[SilverKingDefaultAuthorizer]" + NoopAuthorizerImpl.class.getCanonicalName();
  }

  @Override
  public AuthResult syncAuthorize(Optional<String> authenticated, byte[] requestedUser) {
    if (authenticated.isPresent()) {
      return Authorizer.createAuthSuccessResult(authenticated.get());
    } else {
      return Authorizer.createAuthFailedResult(AuthFailedAction.GO_WITHOUT_AUTH, null);
    }
  }

}

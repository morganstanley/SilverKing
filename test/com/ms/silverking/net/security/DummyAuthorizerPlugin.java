package com.ms.silverking.net.security;

import java.util.Optional;

import com.ms.silverking.text.ObjectDefParser2;

public class DummyAuthorizerPlugin extends Authorizer {
  private final String id;
  final AuthorizationFailedAction failedAction;
  final boolean shouldPass;

  static {
    ObjectDefParser2.addParser(new DummyAuthorizerPlugin());
  }

  public DummyAuthorizerPlugin(String id, AuthorizationFailedAction action, boolean shouldPass) {
    this.id = id;
    this.failedAction = action;
    this.shouldPass = shouldPass;
  }

  // For reflection via ObjectToDefParser
  public DummyAuthorizerPlugin() {
    this.id = "reflectedDummy";
    this.failedAction = null;
    this.shouldPass = true;
  }

  @Override
  public String getName() {
    return "Dummy" + this.id;
  }

  @Override
  public AuthorizationResult syncAuthorize(Optional<String> authenticated, byte[] requestedUser) {
    if (shouldPass) {
      return Authorizer.createAuthSuccessResult(authenticated.get());
    } else {
      return Authorizer.createAuthFailedResult(failedAction, null);
    }
  }
}

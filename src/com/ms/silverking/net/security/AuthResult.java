package com.ms.silverking.net.security;

import java.util.Optional;

public class AuthResult {
  private String authId;
  // We encapsulate AuthFailedAction in AuthResult so that Authenticator can define different actions for different
  // authentication situation
  // this action currently is only used if <i>isFailed()</i> returns true
  private AuthFailedAction failedAction;
  private Throwable cause;

  /**
   * @param authId AuthenticationId; <b>null</b> if authentication fails
   * @param action action for Silverking to take when authentication fails
   */
  AuthResult(String authId, AuthFailedAction action, Throwable cause) {
    this.authId = authId;
    this.failedAction = action;
    this.cause = cause;
  }

  public boolean isSuccessful() {
    return getAuthId().isPresent();
  }

  public boolean isFailed() {
    return !isSuccessful();
  }

  public Optional<String> getAuthId() {
    return Optional.ofNullable(authId);
  }

  public AuthFailedAction getFailedAction() {
    return failedAction;
  }

  public Optional<Throwable> getFailCause() {
    return Optional.ofNullable(cause);
  }
}

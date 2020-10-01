package com.ms.silverking.net.security;

import java.util.Optional;

public class AuthenticationResult {
  private String authenticatedId;
  // We encapsulate AuthFailedAction in AuthResult so that Authenticator can define different actions for different
  // authentication situation
  // this action currently is only used if <i>isFailed()</i> returns true
  private AuthenticationFailedAction failedAction;
  private Throwable cause;

  /**
   * @param authenticatedId AuthenticationId; <b>null</b> if authentication fails
   * @param action action for Silverking to take when authentication fails
   */
  AuthenticationResult(String authenticatedId, AuthenticationFailedAction action, Throwable cause) {
    this.authenticatedId = authenticatedId;
    this.failedAction = action;
    this.cause = cause;
  }

  public boolean isSuccessful() {
    return getAuthenticatedId().isPresent();
  }

  public boolean isFailed() {
    return !isSuccessful();
  }

  public Optional<String> getAuthenticatedId() {
    return Optional.ofNullable(authenticatedId);
  }

  public AuthenticationFailedAction getFailedAction() {
    return failedAction;
  }

  public Optional<Throwable> getFailCause() {
    return Optional.ofNullable(cause);
  }
}

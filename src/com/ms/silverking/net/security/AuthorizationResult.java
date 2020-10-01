package com.ms.silverking.net.security;

import java.util.Optional;

public class AuthorizationResult {
  private String authorizedId;
  private AuthorizationFailedAction failedAction;
  private Throwable cause;

  AuthorizationResult(String authorizedId, AuthorizationFailedAction action, Throwable cause) {
    this.authorizedId = authorizedId;
    this.failedAction = action;
    this.cause = cause;
  }

  public boolean isSuccessful() {
    return getAuthorizedId().isPresent();
  }

  public boolean isFailed() {
    return !isSuccessful();
  }

  public Optional<String> getAuthorizedId() {
    return Optional.ofNullable(authorizedId);
  }

  public AuthorizationFailedAction getFailedAction() {
    return failedAction;
  }

  public Optional<Throwable> getFailCause() {
    return Optional.ofNullable(cause);
  }
}

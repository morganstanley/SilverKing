package com.ms.silverking.net.security;

public class NonRetryableAuthFailedException extends AuthFailedException {
  public NonRetryableAuthFailedException(String message) {
    super(message);
  }

  public NonRetryableAuthFailedException(Throwable cause) {
    super(cause);
  }

  public NonRetryableAuthFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public boolean isRetryable() {
    return false;
  }
}
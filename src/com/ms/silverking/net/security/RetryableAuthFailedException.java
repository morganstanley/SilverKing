package com.ms.silverking.net.security;

public class RetryableAuthFailedException extends AuthFailedException {

  public RetryableAuthFailedException(String message) {
    super(message);
  }

  public RetryableAuthFailedException(Throwable cause) {
    super(cause);
  }

  public RetryableAuthFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public boolean isRetryable() {
    return true;
  }
}

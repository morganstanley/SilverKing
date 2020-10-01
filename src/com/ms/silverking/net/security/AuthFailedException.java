package com.ms.silverking.net.security;

public abstract class AuthFailedException extends Exception {
  public AuthFailedException() {
  }

  public AuthFailedException(String message) {
    super(message);
  }

  public AuthFailedException(Throwable cause) {
    super(cause);
  }

  public AuthFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public abstract boolean isRetryable();
}

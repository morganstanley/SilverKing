package com.ms.silverking.net.security;

public class AuthenticationFailError extends Error {
  public AuthenticationFailError() {
  }

  public AuthenticationFailError(String message) {
    super(message);
  }

  public AuthenticationFailError(Throwable cause) {
    super(cause);
  }

  public AuthenticationFailError(String message, Throwable cause) {
    super(message, cause);
  }
}

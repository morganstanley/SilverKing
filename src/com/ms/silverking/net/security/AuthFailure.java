package com.ms.silverking.net.security;

public class AuthFailure extends Error {
  public AuthFailure() {
  }

  public AuthFailure(String message) {
    super(message);
  }

  public AuthFailure(Throwable cause) {
    super(cause);
  }

  public AuthFailure(String message, Throwable cause) {
    super(message, cause);
  }
}

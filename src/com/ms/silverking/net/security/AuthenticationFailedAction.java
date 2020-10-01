package com.ms.silverking.net.security;

/**
 * The behaviour for Silverking when auth fails for a connection
 */
public enum AuthenticationFailedAction {
  /**
   * Silverking will throw an exception which will NOT be internally caught to have retries
   */
  THROW_NON_RETRYABLE,
  /**
   * Silverking will throw an exception which will be internally caught to have retries
   */
  THROW_RETRYABLE,
  /**
   * Silverking will continue to work without authentication
   * (May be used if authentication is not a must, and this is Silverking's current default behaviour)
   */
  GO_WITHOUT_AUTH
}

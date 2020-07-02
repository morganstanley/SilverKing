package com.ms.silverking.net.security;

/**
 * The behaviour for Silverking when auth fails for a connection
 */
public enum AuthFailedAction {
  /**
   * Silverking will throw <i>AuthenticationFailError</i>, currently Silverking will <b>NOT</b> handle this error
   * (May be used for the clientside behaviour)
   */
  THROW_ERROR,
  /**
   * Silverking will continue to work without authentication
   * (May be used if authentication is not a must, and this is Silverking's current default behaviour)
   */
  GO_WITHOUT_AUTH,
  /**
   * Silverking will absorb this failure and:
   *   - drop the connection (for authentication)
   *   - respond with an error result (for authorization)
   * (May be used for the serverside behaviour)
   */
  ABSORB
}

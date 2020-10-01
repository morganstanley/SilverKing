package com.ms.silverking.net.security;

public enum AuthorizationFailedAction {
  /**
   * SilverKing will throw a exception (for now silverking will catch it in server side and log the error)
   */
  THROW_EXCEPTION,
  /**
   * Default behaviour if no user plugin - silverking will continue the operation despite authorization
   */
  GO_WITHOUT_AUTH,
  /**
   * Silverking will swallow the request and respond with an error; the operation is not started
   */
  RETURN_ERROR_RESPONSE
}

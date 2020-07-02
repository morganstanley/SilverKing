package com.ms.silverking.net.security;

import java.util.Optional;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.util.PropertiesHelper;

/**
 * Plugin point for Authorization
 * In general, using an Authorizer will imply that an Authenticator is also plugged in
 * since you will probably want to authorize against some authenticated user, but this is not enforced
 * in this singleton - if desired, the plugin ought to enforce the presence of an authenticator
 */
public abstract class Authorizer {
  public static final String authorizerImplProperty = Authorizer.class.getPackage().getName() + ".AuthorizerImplSKDef";
  private static final String emptyDef = "";

  private static Authorizer singletonAuthorizer;
  private static boolean isEnabled;

  static {
    ObjectDefParser2.addParserWithExclusions(Authorizer.class, null, FieldsRequirement.ALLOW_INCOMPLETE, null);
    String authDef = PropertiesHelper.systemHelper.getString(authorizerImplProperty, emptyDef);
    setAuthorizer(authDef);
  }

  public static Authorizer parseSKDef(String skDef) {
    return ObjectDefParser2.parse(skDef, Authenticator.class.getPackage());
  }

  public static boolean isEnabled() { return isEnabled; }

  public static Authorizer getPlugin() {
    if (isEnabled) {
      return singletonAuthorizer;
    } else {
      throw new AuthFailure("Invalid call to getPlugin when Authorizer was not enabled!");
    }
  }

  public static AuthResult createAuthFailedResult(AuthFailedAction action, Throwable cause) {
    assert action != null;
    return new AuthResult(null, action, cause);
  }

  public static AuthResult createAuthSuccessResult(String authorizedId) {
    assert authorizedId != null;
    return new AuthResult(authorizedId, null, null);
  }

  /**
   * Used for logging debug/error message to locate the concrete Authorizer implementation
   *
   * @return a distinguishable name
   */
  public abstract String getName();

  public final String toSKDef() {
    return ObjectDefParser2.toClassAndDefString(this);
  }

  /**
   * Allow user to inject authorization of Silverking communication between DHTClient and DHTNode
   * (Server) or between two distributed DHTNodes(servers)
   *
   * @param authenticated The user authenticated for the current connection
   * @param requestedUser The user to authorize
   * @return an AuthResult instance, which may define a failure action for a rejected authorization attempt
   */
  public abstract AuthResult syncAuthorize(Optional<String> authenticated, byte[] requestedUser);

  public static void setAuthorizer(String authDef) {
    if (authDef == null || authDef.equals(emptyDef)) {
      isEnabled = false;
    } else {
      isEnabled = true;
      singletonAuthorizer = parseSKDef(authDef);
    }
  }

}

package com.ms.silverking.net.security;

import static com.ms.silverking.testing.Util.getTestMessage;
import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

public class AuthPluginSKReflectionTest {

  @Test
  public void testToSkDefAndParse_successful_getAuthId_getFailedAction() {
    Object[][] testCases = { { new DummyAuthenticatorImpl1("MyDummy1", 1999), true, DummyAuthenticatorImpl1.dummyAuthId,
        AuthenticationFailedAction.THROW_NON_RETRYABLE },
        { new DummyAuthenticatorImpl2(), false, null, DummyAuthenticatorImpl2.failedAction },
        { new NoopAuthenticatorImpl(), false, null, AuthenticationFailedAction.GO_WITHOUT_AUTH }, };

    for (Object[] testCase : testCases) {
      Authenticator authenticator = (Authenticator) testCase[0];
      boolean expectedSuccess = (boolean) testCase[1];
      String expectedId = (String) testCase[2];
      AuthenticationFailedAction expectedAction = (AuthenticationFailedAction) testCase[3];

      String def = authenticator.toSKDef();
      Authenticator parsedAuthenticator = Authenticator.parseSKDef(def);

      //            checkSKDefAndParse(dummyAuthenticator);   // Authenticator needs to override equals for this to
      //            work, will just compare names for now instead
      assertEquals(authenticator.getName(), parsedAuthenticator.getName());

      AuthenticationResult result = parsedAuthenticator.syncAuthenticate(null, true, 0);

      assertEquals(getTestMessage("isSuccessful", "impl=[" + authenticator.getClass().getCanonicalName() + "]"),
          expectedSuccess, result.isSuccessful());
      if (expectedId != null)
        assertEquals(getTestMessage("getAuthId", "impl=[" + authenticator.getClass().getCanonicalName() + "]"),
            expectedId, result.getAuthenticatedId().get());
      assertEquals(getTestMessage("createAuthFailResult", "impl=[" + authenticator.getClass().getCanonicalName() + "]"),
          expectedAction, result.getFailedAction());
    }
  }

  private byte[] reqUser = "test".getBytes();
  private Optional<String> authUser = Optional.of("test");

  private void testDummyPlugin(DummyAuthorizerPlugin dummy) {
    String def = dummy.toSKDef();
    // Test parse
    Authorizer parsedPlugin = Authorizer.parseSKDef(def);
    assert (parsedPlugin instanceof DummyAuthorizerPlugin);

    // Test behaviour
    AuthorizationResult res = dummy.syncAuthorize(authUser, reqUser);
    assert (res.isSuccessful() == dummy.shouldPass);
    if (res.isSuccessful()) {
      assert (res.getAuthorizedId().equals(authUser));
    } else {
      assert (!res.getAuthorizedId().isPresent());
    }
    assert (res.getFailedAction() == dummy.failedAction);
  }

  @Test
  public void testAuthorizerReflection() {
    DummyAuthorizerPlugin goodDummy = new DummyAuthorizerPlugin("_good", null, true);
    DummyAuthorizerPlugin badDummy = new DummyAuthorizerPlugin("_bad", AuthorizationFailedAction.THROW_EXCEPTION, false);
    DummyAuthorizerPlugin absorbDummy = new DummyAuthorizerPlugin("_return_error_response", AuthorizationFailedAction.RETURN_ERROR_RESPONSE, false);
    DummyAuthorizerPlugin ignoreDummy = new DummyAuthorizerPlugin("_go_without_auth", AuthorizationFailedAction.GO_WITHOUT_AUTH,
        false);

    DummyAuthorizerPlugin[] toTest = { goodDummy, badDummy, absorbDummy, ignoreDummy };
    for (DummyAuthorizerPlugin dummy : toTest) {
      testDummyPlugin(dummy);
    }
  }

  @Test
  public void testNoopAuthorizerPlugin() {
    NoopAuthorizerImpl plugin = new NoopAuthorizerImpl();

    String def = plugin.toSKDef();
    // Test parse
    Authorizer parsedPlugin = Authorizer.parseSKDef(def);
    assert (parsedPlugin instanceof NoopAuthorizerImpl);

    plugin.syncAuthorize(authUser, reqUser);

  }

}

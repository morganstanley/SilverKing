package com.ms.silverking.net.security;

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

import java.net.Socket;
import java.util.Optional;

public abstract class Authenticator {
    public static final String authImplProperty = Authenticator.class.getPackage().getName() +".AuthImplSKDef";
    static {
        ObjectDefParser2.addParserWithExclusions(Authenticator.class, null, FieldsRequirement.ALLOW_INCOMPLETE, null);
    }
    public String toSKDef() {
        return ObjectDefParser2.toClassAndDefString(this);
    }
    public static Authenticator parseSKDef(String skDef) {
        return ObjectDefParser2.parse(skDef, Authenticator.class.getPackage());
    }

    public static Authenticator getAuthenticator(String property) {
        if (property == null) {
            return new NoopAuthenticatorImpl();
        } else {
            return parseSKDef(property);
        }
    }
    
    public static class AuthResult {
        private String authId;
        // We encapsulate AuthFailedAction in AuthResult so that Authenticator can define different actions for different authentication situation
        // this action currently is only used if <i>isFailed()</i> returns true
        private AuthFailedAction failedAction;
        private Throwable cause;

        /**
         * @param authId AuthenticationId; <b>null</b> if authentication fails
         * @param action action for Silverking to take when authentication fails
         */
        private AuthResult(String authId, AuthFailedAction action, Throwable cause) {
            this.authId = authId;
            this.failedAction = action;
            this.cause = cause;
        }

        public boolean isSuccessful() {
            return getAuthId().isPresent();
        }
        public boolean isFailed() {
            return !isSuccessful();
        }
        public Optional<String> getAuthId() {
            return Optional.ofNullable(authId);
        }
        public AuthFailedAction getFailedAction() {
            return failedAction;
        }
        public Optional<Throwable> getFailCause() {
            return Optional.ofNullable(cause);
        }
    }

    private static AuthFailedAction defAction = AuthFailedAction.THROW_ERROR;
    // Factory constructor for AUTH_SUCCESS and AUTH_FAIL
    public static AuthResult createAuthSuccessResult(String authId) {
        assert authId != null;
        return new AuthResult(authId, defAction, null);
    }
    public static AuthResult createAuthFailResult(AuthFailedAction action, Throwable cause) {
        assert action != null;
        return new AuthResult(null, action, cause);
    }
    public static AuthResult createAuthFailResult(AuthFailedAction action) {
        return createAuthFailResult(action, null);
    }

    /**
     * The behaviour for Silverking when authentication fails for a connection
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
         * Silverking will absorb/give up this connection, and will still working
         * (May be used for the serverside behaviour)
         */
        ABSORB_CONNECTION
    }

    /**
     * Used for logging debug/error message to locate the concrete Authenticator implementation
     * @return a distinguishable name
     */
    public abstract String getName();

    /**
     * A method to implicitly indicate the thread safety of Authenticator implementation;
     * Silverking will use this method to create a ThreadLocal Authenticator instance
     *
     * <br/>
     * It's implementor's responsibility to decide to
     *  <b>make a real deep copy/clone (instance is not thread-safe to be shared)<b/> OR
     *  <b>simply return "this"(instance is thread-safe to be shared)</b>
     *
     * @return a local "copy" of Authenticator
     */
    public abstract Authenticator createLocalCopy();

    /**
     * Silverking itself may give up and cancel syncAuthenticate() if it takes too long,
     * and this function is used to indicate when Silverking cancels the syncAuthenticate() on its behalf, what's the next action to do
     *
     * @param serverside <b>true</b> if authenticating in the DHTNodes who acts like server(receives data); <b>false</b> in the DHTNodes
     *                  who acts like client(send data), or DHTClient
     * @return corresponding action for silverking to take when timeout
     */
    public abstract AuthFailedAction onAuthTimeout(boolean serverside);

    /**
     * Allow user to inject authentication before the Silverking network communication between DHTClient and DHTNode(Server) or between two distributed DHTNodes(servers)
     *
     * This function is sync and called before Silverking starts to send data on the given socket; The given socket is
     * connected and sync(blocking) at this moment; After this function call, silverking will turn the socket into async(non-blocking) mode,
     * and user shall <b>NOT</b> re-used the given socket
     *
     * <br/><b>NOTE:</b> Currently, Silverking gives the full control to Authenticator, so it's Authenticator's responsibility to handle the timeout
     * (Silverking will simply be blocked on this method)
     *
     * @param unauthNetwork the <b>connected</b> and <b>sync</b> raw Socket between client and server, which has been not authenticated yet
     * @param serverside <b>true</b> if authenticating in the DHTNodes who acts like server(receives data); <b>false</b> in the DHTNodes
     *                   who acts like client(send data), or DHTClient
     * @param timeoutInMillisecond the maximum time to execute this authentication;
     *                             when timeout, a corresponding AuthResult shall be returned
     *
     * @return a <b>String</b> id of the authentication succeeds, or an <b>empty</b> if authentication fails
     *
     */
    public abstract AuthResult syncAuthenticate(final Socket unauthNetwork, boolean serverside, int timeoutInMillisecond);
}

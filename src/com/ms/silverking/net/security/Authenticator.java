package com.ms.silverking.net.security;

import java.net.Socket;
import java.util.Optional;

public interface Authenticator {
    public class AuthResult {
        private String authId;
        // We encapsulate AuthFailedAction in AuthResult so that Authenticator can define different actions for different authentication situation
        // this action currently is only used if <i>isFailed()</i> returns true
        private AuthFailedAction failedAction;

        /**
         * @param authId AuthenticationId; <b>null</b> if authentication fails
         * @param action action for Silverking to take when authentication fails
         */
        private AuthResult(String authId, AuthFailedAction action) {
            this.authId = authId;
            this.failedAction = action;
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
    }

    // Used by AUTH_SUCCESS case
	public AuthFailedAction defAction = AuthFailedAction.THROW_ERROR;
    // Factory constructor for AUTH_SUCCESS and AUTH_FAIL
    public static AuthResult createAuthSuccessResult(String authId) {
        assert authId != null;
        return new AuthResult(authId, defAction);
    }

    public static AuthResult createAuthFailResult(AuthFailedAction action) {
        assert action != null;
        return new AuthResult(null, action);
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
    public String getName();

    /**
     * Silverking itself may give up and cancel syncAuthenticate() if it takes too long,
     * and this function is used to indicate when Silverking cancels the syncAuthenticate() on its behalf, what's the next action to do
     *
     * @param serverside <b>true</b> if authenticating in the DHTNodes who acts like server(receives data); <b>false</b> in the DHTNodes
     *                  who acts like client(send data), or DHTClient
     * @return corresponding action for silverking to take when timeout
     */
    public AuthFailedAction onAuthTimeout(boolean serverside);

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
    public AuthResult syncAuthenticate(final Socket unauthNetwork, boolean serverside, int timeoutInMillisecond);
}

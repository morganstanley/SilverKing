package com.ms.silverking.net.security;

import com.ms.silverking.net.async.ConnectionListener;

import java.nio.channels.SocketChannel;

public class ConnectionAbsorbException extends Exception {
    // Meta information for absorb
    private SocketChannel absorbedRawConn;
    private ConnectionListener listenerForAbsorbedConn;
    private boolean serverside;
    private Throwable cause;

    public ConnectionAbsorbException(SocketChannel absorbedRawConn, ConnectionListener listenerForAbsorbedCon, boolean serverside, Throwable cause) {
        this.absorbedRawConn = absorbedRawConn;
        this.listenerForAbsorbedConn = listenerForAbsorbedCon;
        this.serverside = serverside;
        this.cause = cause;
    }
    
    public String getAbsorbedInfoMessage() {
        String localEndpoint = absorbedRawConn.socket().getLocalAddress().toString();
        String remoteEndpoing = absorbedRawConn.socket().getRemoteSocketAddress().toString();
        String side = serverside ? "ServerSide" : "ClientSide";
        String listenerInfo = listenerForAbsorbedConn.toString();
        String causeMsg = cause!=null ? " => cause: {" + cause.getMessage() + "}" : "";

        return "Connection between [" + localEndpoint + "(local)] and [" + remoteEndpoing + "(remote)] is absorbed in ["
                + side + "] with listener [" + listenerInfo + "]" + causeMsg;

    }
}

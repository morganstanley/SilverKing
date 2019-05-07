package com.ms.silverking.net.security;

import com.ms.silverking.net.async.ConnectionListener;

import java.nio.channels.SocketChannel;

public class ConnectionAbsorbException extends Exception {
	// Meta information for absorb
	private SocketChannel absorbedRawConn;
	private ConnectionListener listenerForAbsorbedConn;
	private boolean serverside;
	
	public ConnectionAbsorbException(SocketChannel absorbedRawConn, ConnectionListener listenerForAbsorbedCon, boolean serverside) {
		this.absorbedRawConn = absorbedRawConn;
		this.listenerForAbsorbedConn = listenerForAbsorbedCon;
		this.serverside = serverside;
	}
	
	public String getAbsorbedInfoMessage() {
		String localEndpoint = absorbedRawConn.socket().getLocalAddress().toString();
		String remoteEndpoing = absorbedRawConn.socket().getRemoteSocketAddress().toString();
		String side = serverside ? "ServerSide" : "ClientSide";
		String listenerInfo = listenerForAbsorbedConn.toString();
		
		return "Connection between [" + localEndpoint + "(local)] and [" + remoteEndpoing + "(remote)] is absorbed in ["
				+ side + "] with listener [" + listenerInfo + "]";
	}
}

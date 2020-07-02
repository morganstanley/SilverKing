package com.ms.silverking.net.security;

import com.ms.silverking.net.async.ConnectionListener;

import java.nio.channels.SocketChannel;

public class ConnectionAbsorbException extends Exception {
  // Meta information for absorb
  private SocketChannel absorbedRawConn;
  private String connInfo;
  private ConnectionListener listenerForAbsorbedConn;
  private boolean serverside;
  private Throwable cause;

  public ConnectionAbsorbException(SocketChannel absorbedRawConn, ConnectionListener listenerForAbsorbedCon,
      boolean serverside, Throwable cause) {
    this(absorbedRawConn, absorbedRawConn.socket() != null ? absorbedRawConn.socket().toString() : "nullSock",
        listenerForAbsorbedCon, serverside, cause);
  }

  public ConnectionAbsorbException(SocketChannel absorbedRawConn, String absorbedConnStr,
      ConnectionListener listenerForAbsorbedCon, boolean serverside, Throwable cause) {
    super(cause);
    this.absorbedRawConn = absorbedRawConn;
    this.connInfo = absorbedConnStr;
    this.listenerForAbsorbedConn = listenerForAbsorbedCon;
    this.serverside = serverside;
    this.cause = cause;
  }

  public String getAbsorbedInfoMessage() {
    String side = serverside ? "ServerSide" : "ClientSide";
    String listenerInfo = listenerForAbsorbedConn != null ? listenerForAbsorbedConn.toString() : "N/A";
    String causeMsg = cause != null ? " => cause: {" + cause.getMessage() + "}" : "";

    return "Connection " + connInfo + " is absorbed in [" + side + "] with listener [" + listenerInfo + "]" + causeMsg;

  }
}

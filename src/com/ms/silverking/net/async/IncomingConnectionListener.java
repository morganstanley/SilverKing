package com.ms.silverking.net.async;

/**
 * Implemented by classes that want AsyncServer to inform it when a 
 * new connection is accepted.
 */
public interface IncomingConnectionListener<T extends Connection> {
	public void incomingConnection(T connection);
}

package com.ms.silverking.net.async;

import java.net.InetSocketAddress;

/**
 * Receives notifications of connection events:
 * 	send success, send failure, receive failure
 */
public interface ConnectionListener {
	public void disconnected(Connection connection, InetSocketAddress dest, Object disconnectionData);
}

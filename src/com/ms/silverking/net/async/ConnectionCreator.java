package com.ms.silverking.net.async;

import java.nio.channels.SocketChannel;

import com.ms.silverking.thread.lwt.LWTPool;

/**
 * Called by AsyncBase when to establish a connection for the newly
 * accepted Channel.
 */
public interface ConnectionCreator<T extends Connection> {
	public T createConnection(SocketChannel channel, 
								SelectorController<T> selectorController,
								ConnectionListener connectionListener, 
								LWTPool lwtPool, boolean debug);
}

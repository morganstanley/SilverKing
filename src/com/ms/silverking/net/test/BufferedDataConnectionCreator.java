package com.ms.silverking.net.test;

import java.nio.channels.SocketChannel;

import com.ms.silverking.net.async.ConnectionCreator;
import com.ms.silverking.net.async.ConnectionListener;
import com.ms.silverking.net.async.SelectorController;
import com.ms.silverking.thread.lwt.LWTPool;

/**
 * ConnectionCreator for BufferedData.
 */
public class BufferedDataConnectionCreator implements ConnectionCreator<BufferedDataConnection> {
	private final BufferedDataReceiver	bufferedDataReceiver;
	
	public BufferedDataConnectionCreator(BufferedDataReceiver bufferedDataReceiver) {
		this.bufferedDataReceiver = bufferedDataReceiver;
	}
	
	@Override
	public BufferedDataConnection createConnection(SocketChannel channel, 
									SelectorController<BufferedDataConnection> selectorController,
									ConnectionListener connectionListener, LWTPool lwtPool, boolean debug) {
		return new BufferedDataConnection(channel, selectorController, connectionListener, bufferedDataReceiver);
	}
}

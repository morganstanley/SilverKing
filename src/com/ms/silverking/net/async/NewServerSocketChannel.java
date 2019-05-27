package com.ms.silverking.net.async;

import java.nio.channels.ServerSocketChannel;

/**
 * Wraps a ServerSocketChannel with a ChannelRegistrationWorker so that
 * ChannelRegistrationWorker can be informed when this channel is registered.
 */
final class NewServerSocketChannel {
	private final ServerSocketChannel		serverSocketChannel;
	private final ChannelRegistrationWorker	channelRegistrationWorker;
	
	public NewServerSocketChannel(ServerSocketChannel serverSocketChannel,
								ChannelRegistrationWorker channelRegistrationWorker) {
		this.serverSocketChannel = serverSocketChannel;
		this.channelRegistrationWorker = channelRegistrationWorker;
	}
	
	public ServerSocketChannel getServerSocketChannel() {
		return serverSocketChannel;
	}
	
	public ChannelRegistrationWorker getChannelRegistrationWorker() {
		return channelRegistrationWorker;
	}
	
	public String toString() {
		return serverSocketChannel +":"+ channelRegistrationWorker;
	}
}

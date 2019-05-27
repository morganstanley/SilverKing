package com.ms.silverking.net.async;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import com.ms.silverking.thread.lwt.LWTPool;

/**
 * Creates DefaultConnections.
 */
public class DefaultConnectionCreator implements ConnectionCreator<DefaultConnection> {
	private final ReceiverProvider	receiverProvider;
	private final Receiver			defaultReceiver;
	
	public DefaultConnectionCreator(Receiver defaultReceiver, 
						ReceiverProvider receiverProvider) {
		this.defaultReceiver = defaultReceiver;
		this.receiverProvider = receiverProvider;
	}
	
	public DefaultConnectionCreator(Receiver defaultReceiver) {
		this(defaultReceiver, null);
	}
	//////////////////////////////////////////////////////////////////////
	
	private Receiver getReceiver(SocketChannel channel) {
		return getReceiver((InetSocketAddress)channel.socket().getRemoteSocketAddress());
	}
	
	private Receiver getReceiver(InetSocketAddress addr) {		
		if (receiverProvider != null) {
			Receiver	receiver;
			
			receiver = receiverProvider.getReceiver(addr);
			if (receiver != null) {
				return receiver;
			} else {
				return defaultReceiver;
			}
		} else {
			return defaultReceiver;
		}
	}
	
	//////////////////////////////////////////////////////////////////////
	
	@Override
	public DefaultConnection createConnection(SocketChannel channel, 
								SelectorController<DefaultConnection> selectorController,
								ConnectionListener connectionListener, LWTPool lwtPool, boolean debug) {
		return new DefaultConnection(channel, selectorController, connectionListener,
					getReceiver(channel));
	}
}

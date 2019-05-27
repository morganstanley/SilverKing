package com.ms.silverking.net.async;

import java.net.InetSocketAddress;

/**
 * Implemented by users desiring to specify the Receiver for an
 * incoming connection.
 */
public interface ReceiverProvider {
	/**
	 * Given a InetSocketAddress, return the Receiver for
	 * this address.
	 * @param addr
	 * @return
	 */
	public Receiver getReceiver(InetSocketAddress addr);
}

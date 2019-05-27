package com.ms.silverking.net.async;

/**
 */
public interface Receiver {
	public void receive(Connection sourceNode, byte[] msg, int length); 
}

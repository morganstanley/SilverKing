package com.ms.silverking.net.async;

import java.net.InetSocketAddress;

/**
 * Accepts notifications of suspect hosts.
 */
public interface SuspectAddressListener {
	public void addSuspect(InetSocketAddress addr, Object cause);
    public void removeSuspect(InetSocketAddress addr);
}

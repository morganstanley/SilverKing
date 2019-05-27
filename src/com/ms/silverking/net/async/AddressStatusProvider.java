package com.ms.silverking.net.async;

import java.net.InetSocketAddress;

/**
 * Provides information on the status of destinations.
 */
public interface AddressStatusProvider {
	public boolean isHealthy(InetSocketAddress addr);
	public boolean isAddressStatusProviderThread();
}

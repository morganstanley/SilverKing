package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;

/**
 * Implemented by classes capable of providing a ClientDHTConfiguration.
 */
public interface ClientDHTConfigurationProvider {
	@OmitGeneration
    public ClientDHTConfiguration getClientDHTConfiguration();
}

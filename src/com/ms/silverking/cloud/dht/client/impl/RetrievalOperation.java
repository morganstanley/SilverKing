package com.ms.silverking.cloud.dht.client.impl;

import java.util.Collection;

import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;

/**
 * Represents a retrieval of some type of data or metadata from the DHT.
 * Semantics depend on the type of retrieval and the RetrievalOptions.
 */
class RetrievalOperation<K> extends KeyedNamespaceOperation<K> {
	/**
	 * Construct a RetrievalOperation from the given arguments
	 * @param group
	 * @param retrievalType
	 * @param retrievalOptions
	 */
	RetrievalOperation(ClientNamespace namespace, Collection<K> keys, RetrievalOptions retrievalOptions) {
		super(ClientOpType.RETRIEVE, namespace, keys, retrievalOptions);
	}
		
	/**
	 * retrievalOptions getter
	 * @return retrievalOptions
	 */
	public final RetrievalOptions retrievalOptions() {
		return (RetrievalOptions)options;
	}
	
    @Override
    OpTimeoutController getTimeoutController() {
        return ((RetrievalOptions)options).getOpTimeoutController();
    }
	
	@Override
	public String toString() {
		StringBuilder	sb;
		
		sb = new StringBuilder();
		sb.append(super.toString());
		sb.append(':');
		sb.append(((RetrievalOptions)options).toString());
		return sb.toString();
	}
}

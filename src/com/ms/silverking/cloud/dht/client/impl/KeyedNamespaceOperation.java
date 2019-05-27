package com.ms.silverking.cloud.dht.client.impl;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.OperationOptions;

/**
 * Operation on a namespace for a given set of keys. 
 * @param <K> key type
 */
abstract class KeyedNamespaceOperation<K> extends NamespaceOperation {
	private final Set<K>	keys;

	KeyedNamespaceOperation(ClientOpType opType, ClientNamespace namespace, Collection<? extends K> keys,
	                        OperationOptions options) {
		super(opType, namespace, options);
		this.keys = ImmutableSet.copyOf(keys);
		// FUTURE - think about whether we can get rid of this copy
		// have a raw mode?
		// but how would we ensure the uniqueness? look for a set?
	}

	int size() {
		return keys.size();
	}
	
	final Set<K> getKeys() {
		return keys;
	}
}

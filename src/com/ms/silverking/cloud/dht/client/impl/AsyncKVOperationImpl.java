package com.ms.silverking.cloud.dht.client.impl;



/**
 * AsyncOperationImpl that has keys and values. Stores the serDesGroup
 * used for the key and value types.
 *
 * @param <K> key type
 * @param <V> value type
 */
abstract class AsyncKVOperationImpl<K,V> extends AsyncKeyedOperationImpl<K> {
	protected final NamespacePerspectiveOptionsImpl<K,V>   nspoImpl;

	public AsyncKVOperationImpl(KeyedNamespaceOperation<K> operation, ClientNamespace namespace, 
	                            NamespacePerspectiveOptionsImpl<K,V> nspoImpl, long curTime,
	                            byte[] originator) {
		super(operation, nspoImpl.getKeyCreator(), namespace, curTime, originator);		
		this.nspoImpl = nspoImpl;
	}
}

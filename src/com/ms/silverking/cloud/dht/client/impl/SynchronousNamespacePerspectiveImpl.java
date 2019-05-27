package com.ms.silverking.cloud.dht.client.impl;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.AsyncRetrieval;
import com.ms.silverking.cloud.dht.client.AsyncValueRetrieval;
import com.ms.silverking.cloud.dht.client.InvalidationException;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.impl.ClientNamespace.OpLWTMode;

class SynchronousNamespacePerspectiveImpl<K,V> extends BaseNamespacePerspectiveImpl<K,V> implements SynchronousNamespacePerspective<K,V> {
    private static final OpLWTMode    opLWTMode;
    
    static {
        opLWTMode = OpLWTMode.AllowUserThreadUsage;
    }
    
	SynchronousNamespacePerspectiveImpl(ClientNamespace clientNamespace, String name, 
	                                    NamespacePerspectiveOptionsImpl<K,V> nspoImpl) {
		super(clientNamespace, name, nspoImpl);
	}	

    @Override
    public Map<K, V> asMap() {
        return new SynchronousNamespacePerspectiveMapView<K,V>(this);
    }    
	
	// reads
	
	@Override
	public Map<K, ? extends StoredValue<V>> retrieve(Set<? extends K> keys,
			RetrievalOptions retrievalOptions) throws RetrievalException {
		AsyncRetrieval<K,V>	asyncRetrieval;

		asyncRetrieval = baseRetrieve(keys, retrievalOptions, opLWTMode);
		asyncRetrieval.waitForCompletion();
		return asyncRetrieval.getStoredValues();
	}
	
    public StoredValue<V> retrieve(K key, RetrievalOptions retrievalOptions) throws RetrievalException {
        Map<K, ? extends StoredValue<V>>  storedValues;
        
        storedValues = retrieve(ImmutableSet.of(key), retrievalOptions);
        if (storedValues.isEmpty()) {
            return null;
        } else {
            return storedValues.values().iterator().next();
        }
    }
	
	@Override
	public Map<K, ? extends StoredValue<V>> get(Set<? extends K> keys,
			GetOptions getOptions) throws RetrievalException {
		return retrieve(keys, getOptions);
	}

	@Override
	public Map<K, V> get(Set<? extends K> keys) throws RetrievalException {
		AsyncValueRetrieval<K,V>	asyncRetrieval;
		
		asyncRetrieval = (AsyncValueRetrieval<K,V>)baseRetrieve(keys, nspoImpl.getDefaultGetOptions(), 
		                                                        opLWTMode);
		asyncRetrieval.waitForCompletion();
		return asyncRetrieval.getValues();
	}
	
	@Override
	public StoredValue<V> get(K key, GetOptions getOptions) throws RetrievalException {
		return retrieve(ImmutableSet.of(key), getOptions).get(key);
	}

	@Override
	public V get(K key) throws RetrievalException {
	    StoredValue<V> storedValue;
	    
	    storedValue = get(key, nspoImpl.getDefaultGetOptions());
	    if (storedValue != null) {
	        return storedValue.getValue();
	    } else {
	        return null;
	    }
	}

	@Override
	public Map<K, ? extends StoredValue<V>> waitFor(Set<? extends K> keys,
			WaitOptions waitOptions) throws RetrievalException {
		return retrieve(keys, waitOptions);
	}

	@Override
	public Map<K, V> waitFor(Set<? extends K> keys) throws RetrievalException {
		AsyncValueRetrieval<K,V>	asyncRetrieval;
		
		asyncRetrieval = (AsyncValueRetrieval<K,V>)baseRetrieve(keys, nspoImpl.getDefaultWaitOptions(), 
		                                                        opLWTMode);
		asyncRetrieval.waitForCompletion();
		return asyncRetrieval.getValues();
	}

	@Override
	public StoredValue<V> waitFor(K key, WaitOptions waitOptions) throws RetrievalException {
		return retrieve(ImmutableSet.of(key), waitOptions).get(key);
	}

	@Override
	public V waitFor(K key) throws RetrievalException {
		return waitFor(key, nspoImpl.getDefaultWaitOptions()).getValue();
	}
	
	// writes
	
	@Override
	public void put(Map<? extends K, ? extends V> values, PutOptions putOptions)
			throws PutException {
		basePut(values, putOptions, opLWTMode).waitForCompletion();
	}
	
	@Override
	public void put(Map<? extends K, ? extends V> values) throws PutException {
		put(values, nspoImpl.getDefaultPutOptions());
	}
	
	@Override
	public void put(K key, V value, PutOptions putOptions) throws PutException {
		put(ImmutableMap.of(key, value), putOptions);
	}

	@Override
	public void put(K key, V value) throws PutException {
		put(key, value, nspoImpl.getDefaultPutOptions());
	}
	
	public void invalidate(Set<? extends K> keys, InvalidationOptions invalidationOptions) throws InvalidationException {
		try {
			baseInvalidation(keys, invalidationOptions, nspoImpl.getValueSerializer(), opLWTMode).waitForCompletion();
		} catch (PutException e) {
			throw new InvalidationException(e.getMessage(), e.getCause(), e.getOperationState(), e.getFailureCause());
		}
	}
	
	public void invalidate(Set<? extends K> keys) throws InvalidationException {
		invalidate(keys, nspoImpl.getDefaultInvalidationOptions());
	}
	
	public void invalidate(K key, InvalidationOptions invalidationOptions) throws InvalidationException {
		invalidate(ImmutableSet.of(key), invalidationOptions);
	}
	
	public void invalidate(K key) throws InvalidationException {
		invalidate(key, nspoImpl.getDefaultInvalidationOptions());
	}
	
	/*
	 * snapshots deprecated for now
    @Override
    public void snapshot(long version) throws SnapshotException {
        try {
            baseSnapshot(version, null).waitForCompletion();
        } catch (OperationException oe) {
            throw (SnapshotException)oe;
        }
    }

    @Override
    public void snapshot() throws SnapshotException {
        snapshot(SystemTimeUtil.systemTimeSource.absTimeMillis());
    }
    */
    
	/*
    @Override
    public void syncRequest(long version) throws SyncRequestException {
        try {
            baseSyncRequest(version, null).waitForCompletion();
        } catch (OperationException oe) {
            throw (SyncRequestException)oe;
        }
    }

    @Override
    public void syncRequest() throws SyncRequestException {
        syncRequest(getAbsMillisTimeSource().absTimeMillis());
    }
    */
}


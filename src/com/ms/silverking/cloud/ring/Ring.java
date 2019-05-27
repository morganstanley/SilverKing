package com.ms.silverking.cloud.ring;

import java.util.Collection;

/**
 * Implemented by classes that support a logical ring divided into
 * regions owned by members of the ring.
 * 
 * @param <K> key type
 * @param <T> member type
 */
public interface Ring<K,T> {
	public void put(K key, T member);
	public T getOwner(K key);
    public Collection<T> getOwners(K minKey, K maxKey);
	public Collection<T> getMembers();
	public int numMembers();
	public T removeOwner(Long key);
}

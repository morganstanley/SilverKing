package com.ms.silverking.collection;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConcurrentMap<K,V> implements Map<K,V>{
	private final int			numBuckets;
	private final Map<K,V>[]	maps;
	
	public ConcurrentMap(int numBuckets) {
		this.numBuckets = numBuckets;
		maps = new Map[numBuckets];
		for (int i = 0; i < maps.length; i++) {
			maps[i] = new ConcurrentSkipListMap<K,V>();
		}
	}
	
	private final int keyToBucket(Object key) {
		return key.hashCode() % numBuckets;
	}
	
	@Override
	public void clear() {
		for (int i = 0; i < maps.length; i++) {
			maps[i].clear();
		}
	}

	@Override
	public boolean containsKey(Object key) {
		return maps[keyToBucket(key)].containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		for (int i = 0; i < maps.length; i++) {
			if (maps[i].containsValue(value)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		Set<Entry<K, V>>	set;
		
		set = new HashSet<Entry<K, V>>();
		for (int i = 0; i < maps.length; i++) {
			set.addAll(maps[i].entrySet());
		}
		return set;
	}

	@Override
	public V get(Object key) {
		return maps[keyToBucket(key)].get(key);
	}

	@Override
	public boolean isEmpty() {
		for (int i = 0; i < maps.length; i++) {
			if (!maps[i].isEmpty()) {
				return false;
			}
		}
		return true;
	}

	@Override
	public Set<K> keySet() {
		Set<K>	set;
		
		set = new HashSet<K>();
		for (int i = 0; i < maps.length; i++) {
			set.addAll(maps[i].keySet());
		}
		return set;
	}

	@Override
	public V put(K key, V value) {
		return maps[keyToBucket(key)].put(key, value);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		for (Entry<? extends K,? extends V> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public V remove(Object key) {
		return maps[keyToBucket(key)].remove(key);
	}

	@Override
	public int size() {
		int	size;
		
		size = 0;
		for (int i = 0; i < maps.length; i++) {
			size += maps[i].size();
		}
		return size;
	}

	@Override
	public Collection<V> values() {
		Set<V>	set;
		
		set = new HashSet<V>();
		for (int i = 0; i < maps.length; i++) {
			set.addAll(maps[i].values());
		}
		return set;
	}
	
	class KeyWrapper {
		private final Object	key;
		private final int		hashCode;
		
		KeyWrapper(Object key) {
			this.key = key;
			this.hashCode = key.hashCode();
		}
		
		public int hashCode() {
			return hashCode;
		}
		
		@Override
		public boolean equals(Object obj) {
			return key.equals(obj);
		}
	}
}

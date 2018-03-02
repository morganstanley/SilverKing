
package com.ms.silverking.collection;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;


public final class HashedSetMap<K,V> implements Serializable {
	private final Map<K,Set<V>>		map;
	private final MissingKeyMode    missingKeyMode;
	private final boolean			useConcurrentSets;
	private final ConcurrencyMode	concurrencyMode;
	
	public enum MissingKeyMode {NULL, EMPTY_SET, PUT_EMPTY_SET};
	public enum ConcurrencyMode {NONE, SETS, ALL};

	private static final long serialVersionUID = 7891997492240035542L;
	
	public HashedSetMap(MissingKeyMode missingKeyMode, ConcurrencyMode concurrencyMode) {
		switch (concurrencyMode) {
		case NONE:
			map = new HashMap<K,Set<V>>();
			useConcurrentSets = false;
			break;
		case SETS:
			map = new HashMap<K,Set<V>>();
			useConcurrentSets = true;
			break;
		case ALL:
			map = new ConcurrentHashMap<K,Set<V>>();
			useConcurrentSets = true;
			break;
		default:
			throw new RuntimeException("panic");
		}
		this.missingKeyMode = missingKeyMode;
		this.concurrencyMode = concurrencyMode;
	}
	
	public HashedSetMap<K,V> clone() {
		HashedSetMap<K,V>	h;
		
		h = new HashedSetMap<>(missingKeyMode, concurrencyMode);
		h.addAll(this);
		return h;
	}
	
	public HashedSetMap(MissingKeyMode missingKeyMode) {
		this(missingKeyMode, ConcurrencyMode.NONE);
	}
	
    public HashedSetMap(boolean fillEmptyListOnGet) {
        this(fillEmptyListOnGet ? MissingKeyMode.PUT_EMPTY_SET : MissingKeyMode.NULL);
    }
    
	public HashedSetMap() {
		this(false);
	}
	
	public Set<K> keySet() {
		return map.keySet();
	}
	
	public boolean containsValue(K key, V value) {
		Set<V>	set;
		
		set = getSet(key);
		if (set == null) {
			return false;
		} else {
			return set.contains(value);
		}
	}
	
    public boolean containsKey(K key) {
        return map.containsKey(key);
    }
    
	public int listSize(K key) {
		Set<V>	list;
		
		list = getSet(key);
		if (list != null) {
			return list.size();
		} else {
			return 0;
		}
	}
	
	public void addValue(K key, V value) {
		Set<V>	set;
		
		set = map.get(key);
		if (set == null) {
			if (!useConcurrentSets) {
				set = new HashSet<V>();
			} else {
				set = new ConcurrentSkipListSet<V>();
			}
			map.put(key, set);
		}
		set.add(value);
	}

	public void addValues(K key, Collection<V> values) {
		for (V value : values) {
			addValue(key, value);
		}
	}
	
	public boolean removeValue(K key, V value) {
		Set<V>	list;
		
		list = map.get(key);
		if (list == null) {
			return false;
		} else {
			return list.remove(value);
		}
	}
	
	public void addAll(HashedSetMap<K,V> m) {
		for (K key : m.getKeys()) {
			addValues(key, m.getSet(key));
		}
	}
	
	public V getAnyValue(K key) {
		Set<V>	set;
		int rndIndex, i;
        
        set = map.get(key);
        if (set == null) {
            return null;
        } else {
        	rndIndex = ThreadLocalRandom.current().nextInt( set.size() );
            i = 0;
            for (V item : set) {
                if (i++ == rndIndex) return item;
            }
            throw new RuntimeException("panic");
        }
	}	
	
	public Set<V> getSet(K key) {
		Set<V>	set;
		
		set = map.get(key);
		if (set == null && missingKeyMode != MissingKeyMode.NULL) {
			if (!useConcurrentSets) {
				set = new HashSet<V>();
			} else {
				set = new ConcurrentSkipListSet<V>();
			}
			if (missingKeyMode == MissingKeyMode.PUT_EMPTY_SET) {
			    map.put(key, set);
			}
		}
		return set;
	}
	
	public List<Set<V>> getSets() {
		List<Set<V>>	sets;
		Iterator<Map.Entry<K,Set<V>>>		iterator;
		
		sets = new ArrayList<Set<V>>();
		iterator = map.entrySet().iterator();
		while ( iterator.hasNext() ) {			
			sets.add( iterator.next().getValue() );
		}
		return sets;
	}
	
	public List<K> getKeys() {
		List<K>	keys;
		Iterator<Map.Entry<K,Set<V>>>	iterator;
		
		keys = new ArrayList<K>();
		iterator = map.entrySet().iterator();
		while ( iterator.hasNext() ) {			
			keys.add( iterator.next().getKey() );
		}
		return keys;
	}
	
	public Set<V> removeSet(K key) {
		Set<V>	set;
		
		set = map.remove(key);
		return set;
	}
	
	public int getNumKeys() {
		return map.size();
	}
}

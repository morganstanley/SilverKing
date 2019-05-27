package com.ms.silverking.cloud.ring;


public class TreeMapRing<K, T> {//implements Ring<K, T> {
	/*
	private final TreeMap<K, T>	ringMap;

	public TreeMapRing() {
		ringMap = new TreeMap<K, T>();
	}
	
	@Override
	public void put(K key, T member) {
		ringMap.put(key, member);
	}
	
	@Override
	public T getOwner(K key) {
		Map.Entry<K, T> entry;
		
		entry = ringMap.ceilingEntry(key);
		if (entry == null) {
			entry = ringMap.firstEntry();
		}
		return entry.getValue();
	}
	
	@Override
	public List<T> get(K key, int numMembers) {
		Map.Entry<K, T> entry;
		List<T>	members;
		
		if (numMembers < 1) {
			throw new RuntimeException("numMembers < 1");
		}
		members = new ArrayList<T>(numMembers);
		entry = ringMap.ceilingEntry(key);
		if (entry == null) {
			entry = ringMap.firstEntry();
		}
		members.add(entry.getValue());
		for (int i = 0; i < numMembers - 1; i++) {
			entry = ringMap.higherEntry(entry.getKey());
			if (entry == null) {
				entry = ringMap.firstEntry();
			}
			members.add(entry.getValue());
		}
		return members;
	}
	
	@Override
	public Collection<T> getMembers() {
		return ringMap.values();
	}
	
	@Override
	public int numMembers() {
		return ringMap.size();
	}
	*/
}

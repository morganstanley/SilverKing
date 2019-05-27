package com.ms.silverking.cloud.ring;


public class DummyRing<K, T> {// implements Ring<K, T> {
	/*
	private T	last;
	
	public DummyRing() {
	}
	
	@Override
	public void put(K key, T member) {
		last = member;
	}
	
	@Override
	public T getOwner(K key) {
		return last;
	}
	
	@Override
	public List<T> get(K key, int numMembers) {
		List<T>	members;
		
		if (numMembers < 1) {
			throw new RuntimeException("numMembers < 1");
		}
		members = new ArrayList<T>(numMembers);
		for (int i = 0; i < numMembers; i++) {
			members.add(last);
		}
		return members;
	}
	
	@Override
	public int numMembers() {
		return 1;
	}
	
	@Override
	public Collection<T> getMembers() {
		return null;
	}
	*/
}

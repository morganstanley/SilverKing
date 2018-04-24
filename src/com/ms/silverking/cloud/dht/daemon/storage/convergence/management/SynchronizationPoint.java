package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import com.ms.silverking.id.UUIDBase;

public class SynchronizationPoint extends Action {
	private final String	name;
	
	private SynchronizationPoint(UUIDBase uuid, String name, Action[] upstreamDependencies) {
		super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), upstreamDependencies);
		
		this.name = name;
	}
	
	public static SynchronizationPoint of(String name, Action[] upstreamDependencies) {
		return new SynchronizationPoint(UUIDBase.random(), name, upstreamDependencies);
	}
	
	public static SynchronizationPoint of(String name, Action upstreamDependency) {
		Action[]	upstreamDependencies;
		
		if (upstreamDependency != null) {
			upstreamDependencies = new Action[1];
			upstreamDependencies[0] = upstreamDependency;
		} else {
			upstreamDependencies = new Action[0];
		}
		return of(name, upstreamDependencies);
	}
	
	public static SynchronizationPoint of(String hexString) {
		return of(hexString, new Action[0]);
	}
	
	public String getName() {
		return name;
	}
	
	@Override
	public String toString() {
		return name;
	}
}

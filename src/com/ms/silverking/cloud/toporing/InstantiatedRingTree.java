package com.ms.silverking.cloud.toporing;

import java.util.Map;

import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.collection.Pair;

public class InstantiatedRingTree extends RingTree {
	private final Pair<Long,Long> ringVersionPair;
	
    public InstantiatedRingTree(Topology topology,
			Map<String, TopologyRing> maps, Pair<Long, Long> ringVersionPair,
			long ringCreationTime) {
		super(topology, maps, ringVersionPair.getV1(), ringCreationTime);
		this.ringVersionPair = ringVersionPair;
	}

    public Pair<Long,Long> getRingVersionPair() {
    	return ringVersionPair;
    }
}

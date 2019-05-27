package com.ms.silverking.cloud.dht.daemon;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.net.IPAndPort;

public class CompositeReplicaPrioritizer implements ReplicaPrioritizer {
	private final List<ReplicaPrioritizer>	replicaPrioritizers;
	
	public CompositeReplicaPrioritizer(List<ReplicaPrioritizer> replicaPrioritizers) {
		this.replicaPrioritizers = ImmutableList.copyOf(replicaPrioritizers);
	}

	@Override
	public int compare(IPAndPort r0, IPAndPort r1) {
		for (ReplicaPrioritizer rp : replicaPrioritizers) {
			int	result;
			
			result = rp.compare(r0, r1);
			if (result != 0) {
				return result;
			}
		}
		return 0;
	}
}

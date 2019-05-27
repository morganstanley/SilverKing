package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.net.IPAndPort;

public class ReplicaNaiveIPPrioritizer implements ReplicaPrioritizer {
	public ReplicaNaiveIPPrioritizer() {
	}
	
	@Override
	public int compare(IPAndPort r1, IPAndPort r2) {
		return r1.compareTo(r2);
	}
}
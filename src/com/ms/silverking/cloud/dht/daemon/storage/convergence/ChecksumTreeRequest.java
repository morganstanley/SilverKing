package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.net.IPAndPort;

public class ChecksumTreeRequest {
	private final ConvergencePoint	targetCP;
	private final ConvergencePoint	curCP;
	private final RingRegion		region;
	private final IPAndPort			replica;
	private long					sendTime;
	
    private static final long	checksumTreeRequestTimeout = 1 * 60 * 1000;
	
	public ChecksumTreeRequest(ConvergencePoint targetCP, ConvergencePoint curCP, RingRegion region, IPAndPort replica) {
		this.targetCP = targetCP;
		this.curCP = curCP;
		this.region = region;
		this.replica = replica;
	}
	
	public ConvergencePoint getTargetCP() {
		return targetCP;
	}
	
	public ConvergencePoint getCurCP() {
		return curCP;
	}
	
	public RingRegion getRegion() {
		return region;
	}
	
	public IPAndPort getReplica() {
		return replica;
	}
	
	public void setSent() {
		sendTime = SystemTimeUtil.systemTimeSource.absTimeMillis();
	}
	
	public boolean hasTimedOut() {
		return SystemTimeUtil.systemTimeSource.absTimeMillis() > sendTime + checksumTreeRequestTimeout;
	}

	@Override
	public String toString() {
		return replica +" "+ region;
	}
}
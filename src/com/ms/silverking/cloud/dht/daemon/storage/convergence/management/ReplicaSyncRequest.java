package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.net.IPAndPort;

class ReplicaSyncRequest {
	private final UUIDBase		uuid;
	private final long			ns;
	private final RingRegion	region;
	private final IPAndPort		newOwner;
	private final IPAndPort		oldOwner;
	private long				sendTimeMillis;
	
	ReplicaSyncRequest(long ns, RingRegion region, IPAndPort newOwner, IPAndPort oldOwner) {
		this.uuid = new UUIDBase();
		this.ns = ns;
		this.region = region;
		this.newOwner = newOwner;
		this.oldOwner = oldOwner;
	}
	
	UUIDBase getUUID() {
		return uuid;
	}
	
	long getNS() {
		return ns;
	}

	RingRegion getRegion() {
		return region;
	}

	IPAndPort getNewOwner() {
		return newOwner;
	}

	IPAndPort getOldOwner() {
		return oldOwner;
	}
	
	void setSendTime(long sendTimeMillis) {
		this.sendTimeMillis = sendTimeMillis;
	}
	
	long getSendTime()  {
		return sendTimeMillis;
	}
	
	boolean containsOwner(IPAndPort owner) {
		return owner.equals(newOwner) || owner.equals(oldOwner);
	}
	
	// All current use of this class uses reference equality for performance
	
	@Override
	public int hashCode() {
		return super.hashCode();
		//return Long.hashCode(ns) ^ region.hashCode() ^ newOwner.hashCode() ^ oldOwner.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
		/*
		ReplicaSyncRequest	o;
		
		o = (ReplicaSyncRequest)obj;
		return this.ns == o.ns && this.region.equals(o.region) && this.newOwner.equals(o.newOwner) && this.oldOwner.equals(o.oldOwner);
		*/
	}
	
	@Override
	public String toString() {
		return String.format("%s:%d:%s:%s<=%s:%d", uuid, ns, region, newOwner, oldOwner, sendTimeMillis);
	}	
}
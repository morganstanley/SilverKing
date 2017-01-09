package com.ms.silverking.cloud.zookeeper;

import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.AddrAndPortUtil;
import com.ms.silverking.net.HostAndPort;

public class ZooKeeperConfig {
    private final AddrAndPort[] ensemble;
    
    public ZooKeeperConfig(AddrAndPort[] ensemble) {
        this.ensemble = ensemble;
    }
    
    public ZooKeeperConfig(String def) {
        this(HostAndPort.parseMultiple(def));
    }
    
    public AddrAndPort[] getEnsemble() {
        return ensemble;
    }
    
    @Override
    public String toString() {
        return AddrAndPortUtil.toString(ensemble);
    }

    public String getEnsembleString() {
        return AddrAndPortUtil.toString(ensemble);
    }
    
    @Override
    public int hashCode() {
    	int	hashCode;
    	
    	hashCode = 0;
    	for (AddrAndPort member : ensemble) {
    		hashCode = hashCode ^ member.hashCode();
    	}
    	return hashCode;
    }
    
    @Override
	public boolean equals(Object other) {
    	ZooKeeperConfig	otherZKC;
		
		otherZKC = (ZooKeeperConfig)other;
		if (ensemble.length != otherZKC.ensemble.length) {
			return false;
		} else {
			// Note that this implementation is currently order-sensitive
			for (int i = 0; i < ensemble.length; i++) {
				if (!ensemble[i].equals(otherZKC.ensemble[i])) {
					return false;
				}
			}
			return true;
		}
	}
}

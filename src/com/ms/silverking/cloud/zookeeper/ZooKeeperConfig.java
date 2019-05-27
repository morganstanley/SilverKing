package com.ms.silverking.cloud.zookeeper;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.AddrAndPortUtil;
import com.ms.silverking.net.HostAndPort;

public class ZooKeeperConfig {
    private final AddrAndPort[] ensemble;
    private final String		chroot;
    
    @OmitGeneration
    public ZooKeeperConfig(AddrAndPort[] ensemble, String chroot) {
    	Preconditions.checkNotNull(ensemble, "ensemble null");
        this.ensemble = ensemble;
        this.chroot = (chroot == null ? "" : chroot);
    }
    
    @OmitGeneration
    public ZooKeeperConfig(Pair<AddrAndPort[],String> ensemble_chroot) {
    	this(ensemble_chroot.getV1(), ensemble_chroot.getV2());
    }
    
    @OmitGeneration
    public ZooKeeperConfig(AddrAndPort[] ensemble) {
        this(ensemble, null);
    }
    
    public ZooKeeperConfig(String def) {
        this(parseDef(def));
    }
    
    private static Pair<AddrAndPort[],String> parseDef(String def) {
    	AddrAndPort[]	ensemble;
    	String			ensembleDef;
    	int				chrootIndex;
    	String			chroot;
    	
    	chrootIndex = def.indexOf('/');
    	if (chrootIndex < 0) {
    		ensembleDef = def;
    		chroot = null;
    	} else {
    		ensembleDef = def.substring(0, chrootIndex);
    		chroot = def.substring(chrootIndex);
    	}
		ensemble = HostAndPort.parseMultiple(ensembleDef);
		return new Pair<>(ensemble, chroot);
    }
    
    public AddrAndPort[] getEnsemble() {
        return ensemble;
    }
    
    public String getChroot() {
    	return chroot;
    }
    
    @Override
    public String toString() {
    	return getConnectString();
    }

    public String getConnectString() {
    	return AddrAndPortUtil.toString(ensemble) + chroot;
    }
    
    @Override
    public int hashCode() {
    	return AddrAndPortUtil.hashCode(ensemble) ^ chroot.hashCode();
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
			return chroot.equals(otherZKC.chroot);
		}
	}
}

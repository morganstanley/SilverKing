package com.ms.silverking.cloud.dht.meta;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.meta.Utils;
import com.ms.silverking.cloud.meta.VersionedDefinition;

/**
 * DHT configuration settings. 
 * (For use within the context of a single ZooKeeper ensemble -
 * thus specification of the ensemble is not necessary -
 * as opposed to ClientDHTConfiguration which specifies a
 * ZooKeeper ensemble.) 
 */
public class DHTSKFSConfiguration implements VersionedDefinition {
	private final String	skfsConfigName;
    private final long      version;
    private final long      zxid;
    
    public DHTSKFSConfiguration(String skfsConfigName, long version, long zxid) {
    	this.skfsConfigName = skfsConfigName;
        this.version = version;
        this.zxid = zxid;
    }
    
    public DHTSKFSConfiguration version(long version) {
        return new DHTSKFSConfiguration(skfsConfigName, version, zxid);
    }
    
    public DHTSKFSConfiguration zkid(long zkid) {
        return new DHTSKFSConfiguration(skfsConfigName, version, zkid);
    }
    
    public String getSKFSConfigName() {
        return skfsConfigName;
    }
    
    @Override
    public long getVersion() {
        return version;
    }
    
    public long getZKID() {
        return zxid;
    }

	public static DHTSKFSConfiguration parse(String def, long version) {
		return new DHTSKFSConfiguration(def, version, 0);
	}
	
	@Override
	public String toString() {
		return skfsConfigName;
	}
}

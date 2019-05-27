package com.ms.silverking.cloud.dht.client;

/**
 * Provides a constant version
 */
public class ConstantVersionProvider implements VersionProvider {
    private final long  version;
    
    public ConstantVersionProvider(long version) {
        this.version = version;
    }

    @Override
    public long getVersion() {
        return version;
    }
    
    @Override
    public int hashCode() {
    	return Long.hashCode(version);
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    		
    	if (this.getClass() != o.getClass()) {
    		return false;
    	}
    	
    	ConstantVersionProvider other = (ConstantVersionProvider)o;
    	return this.version == other.version;
    }
}

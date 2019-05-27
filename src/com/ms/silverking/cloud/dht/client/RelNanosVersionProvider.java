package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.time.RelNanosTimeSource;

/**
 * Provides versions from a RelNanosTimeSource.
 */
public class RelNanosVersionProvider implements VersionProvider {
    private final RelNanosTimeSource   relNanosTimeSource;
    
    public RelNanosVersionProvider(RelNanosTimeSource relNanosTimeSource) {
        this.relNanosTimeSource = relNanosTimeSource;
    }
    
    @Override
    public long getVersion() {
        return relNanosTimeSource.relTimeNanos();
    }
    
    @Override
    public int hashCode() {
    	return relNanosTimeSource.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    		
    	if (this.getClass() != o.getClass()) {
    		return false;
    	}
    	
    	RelNanosVersionProvider other = (RelNanosVersionProvider)o;
    	return this.relNanosTimeSource.equals(other.relNanosTimeSource);
    }
}

package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.time.AbsNanosTimeSource;

/**
 * Provides versions from an AbsNanosTimeSource.
 */
public class AbsNanosVersionProvider implements VersionProvider {
    private final AbsNanosTimeSource   absNanosTimeSource;
    
    public AbsNanosVersionProvider(AbsNanosTimeSource absNanosTimeSource) {
        this.absNanosTimeSource = absNanosTimeSource;
    }
    
    @Override
    public long getVersion() {
        return absNanosTimeSource.absTimeNanos();
    }
    
    @Override
    public int hashCode() {
    	return absNanosTimeSource.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
    	if (this == o) {
    		return true;
    	}
    	
    	if (this.getClass() != o.getClass()) {
    		return false;
    	}
    	
    	AbsNanosVersionProvider other = (AbsNanosVersionProvider)o;
    	return this.absNanosTimeSource.equals(other.absNanosTimeSource);
    }
}

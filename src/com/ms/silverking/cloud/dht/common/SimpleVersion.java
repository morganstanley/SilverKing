package com.ms.silverking.cloud.dht.common;


public class SimpleVersion implements Version {
    private final long  version;
    
    public SimpleVersion(long version) {
        this.version = version;
    }
    
    @Override
    public long versionAsLong() {
        return version;
    }
    
    @Override
    public int hashCode() {
        return (int)version;
    }
    
    @Override
    public boolean equals(Object other) {
        Version oVersion;
        
        oVersion = (Version)other;
        return version == oVersion.versionAsLong();
    }
    
    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        sb.append(version);
        return sb.toString();
    }
}

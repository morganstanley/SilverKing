package com.ms.silverking.cloud.dht.common;


public class SimpleNSKeyVersion extends SimpleNSKey {
    private final long  version;
    
    public SimpleNSKeyVersion(Namespace ns, DHTKey key, Version version) {
        super(ns, key);
        this.version = version.versionAsLong();
    }
    
    public SimpleNSKeyVersion(NSKey nsKey, Version version) {
        this(nsKey.getNamespace(), nsKey.getKey(), version);
    }
    
    @Override 
    public int hashCode() {
        return super.hashCode() ^ (int)version;
    }
    
    @Override 
    public boolean equals(Object o) {
        SimpleNSKeyVersion  other;
        
        other = (SimpleNSKeyVersion)o;
        return this.version == other.version && super.equals(other);
    }
}

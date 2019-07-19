package com.ms.silverking.cloud.dht.common;


public class SimpleKeyVersion implements KeyVersion {
    private final DHTKey    key;
    private final Version   version;
    
    public SimpleKeyVersion(DHTKey key, Version version) {
        this.key = key;
        this.version = version;
    }
    
    public SimpleKeyVersion(DHTKey key, long version) {
        this(key, new SimpleVersion(version));
    }
    
    @Override
    public DHTKey getKey() {
        return key;
    }

    @Override
    public Version getVersion() {
        return version;
    }
    
    @Override
    public int hashCode() {
        return (int)(key.getLSL() | version.versionAsLong());
    }
    
    @Override
    public boolean equals(Object other) {
        KeyVersion  oKeyVersion;
        
        oKeyVersion = (KeyVersion)other;
        return key.equals(oKeyVersion.getKey()) && version.equals(oKeyVersion.getVersion());
    }
    
    @Override
    public String toString() {
        return key +":"+ version;
    }
}

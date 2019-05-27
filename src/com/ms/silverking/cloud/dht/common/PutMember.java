package com.ms.silverking.cloud.dht.common;


public class PutMember {
    private final DHTKey    key;
    private final Version   version;
    
    public PutMember(DHTKey key, Version version) {
        this.key = key;
        this.version = version;
    }
    
    public DHTKey getKey() {
        return key;
    }
    
    public Version getVersion() {
        return version;
    }
}

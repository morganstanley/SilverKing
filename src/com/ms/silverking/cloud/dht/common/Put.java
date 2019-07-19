package com.ms.silverking.cloud.dht.common;


public interface Put {
    public DHTKey getKey();
    public Version getVersion();
    /*
    private final Namespace ns;
    private final DHTKey    key;
    private final Version   version;
    private final long      opTimeMillis;
    
    public Retrieval(Namespace ns, DHTKey key, Version version, long opTimeMillis) {
        this.ns = ns;
        this.key = key;
        this.version = version;
        this.opTimeMillis = opTimeMillis;
    }
    
    public Namespace getNamespace() {
        return ns;
    }
    
    public DHTKey getKey() {
        return key;
    }
    
    public Version getVersion() {
        return version;
    }
    
    @Override
    public int hashCode() {
        return key.hashCode() ^ ns.hashCode() ^ version.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        Retrieval   other;
        
        other = (Retrieval)o;
        return other.ns.equals(ns)
            && other.key.equals(key)
            && other.version.equals(version)
            && other.opTimeMillis == opTimeMillis;
    }
    */
}

package com.ms.silverking.cloud.dht.common;


public class SimpleNSKey {
    private final long  key_msl;
    private final long  key_lsl;
    private final long  ns;
    
    public SimpleNSKey(Namespace ns, DHTKey key) {
        key_msl = key.getMSL();
        key_lsl = key.getLSL();
        this.ns = ns.contextAsLong();
    }
    
    @Override 
    public int hashCode() {
        return (int)(key_lsl ^ ns);
    }
    
    @Override 
    public boolean equals(Object o) {
        SimpleNSKey  other;
        
        other = (SimpleNSKey)o;
        return this.key_lsl == other.key_lsl
            && this.key_msl == other.key_msl
            && this.ns == other.ns;
    }
}

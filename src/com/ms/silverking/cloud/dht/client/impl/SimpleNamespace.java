package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.common.Namespace;
import com.ms.silverking.numeric.NumConversion;

public class SimpleNamespace implements Namespace {
    private final long  ns;
    
    public SimpleNamespace(long ns) {
        this.ns = ns;
    }
    
    public SimpleNamespace(byte[] digest) {
        this(NumConversion.bytesToLong(digest));
    }
    
    @Override
    public long contextAsLong() {
        return ns;
    }
    
    @Override 
    public int hashCode() {
        return (int)ns;
    }
    
    @Override
    public boolean equals(Object other) {
        Namespace   oNS;
        
        oNS = (Namespace)other;
        return ns == oNS.contextAsLong();
    }
}

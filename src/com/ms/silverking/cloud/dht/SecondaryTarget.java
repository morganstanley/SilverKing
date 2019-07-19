package com.ms.silverking.cloud.dht;

import com.ms.silverking.cloud.dht.client.SecondaryTargetType;
import com.ms.silverking.text.ObjectDefParser2;

public class SecondaryTarget {
    private final SecondaryTargetType  type;
    private final String               target;
    
    private static final SecondaryTarget   template = new SecondaryTarget(null, "");
    
    static {
        ObjectDefParser2.addParser(template);
    }
    
    public SecondaryTarget(SecondaryTargetType type, String target) {
        this.type = type;
        this.target = target;
    }
    
    public SecondaryTargetType getType() {
        return type;
    }

    public String getTarget() {
        return target;
    }

    @Override
    public String toString() {
        return ObjectDefParser2.objectToString(this);
    }
    
    @Override
    public int hashCode() {
        return type.hashCode() ^ target.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
        SecondaryTarget    o;
        
        o = (SecondaryTarget)other;
        return type == o.type && target.equals(o.target);
    }
}

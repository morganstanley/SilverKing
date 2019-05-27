package com.ms.silverking.numeric;

import com.google.common.base.Preconditions;

public class LongInterval {
    private final long  l1;
    private final long  l2;
    
    public LongInterval(long l1, long l2) {
		Preconditions.checkArgument(l2 >= l1);
        this.l1 = l1;
        this.l2 = l2;
    }
    
    public long getStart() {
        return l1;
    }
    
    public long getEnd() {
        return l2;
    }
    
    public long getSize() {
        return l2 - l1 + 1;
    }
    
    @Override 
    public int hashCode() {
        return NumUtil.longHashCode(l1) ^ NumUtil.longHashCode(l2);
    }
    
    @Override
    public boolean equals(Object o) {
        LongInterval    oLI;
        
        oLI = (LongInterval)o;
        return l1 == oLI.l1 && l2 == oLI.l2;
    }    
    
    @Override
    public String toString() {
    	return "["+ l1 +","+ l2 +"]";
    }
}

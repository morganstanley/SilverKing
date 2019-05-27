package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.List;

import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.Mutability;

public abstract class AbstractChecksumNode implements ChecksumNode {
    protected final RingRegion  ringRegion;
    protected Mutability        mutability;
    
    public AbstractChecksumNode(RingRegion ringRegion, Mutability mutability) {
        this.ringRegion = ringRegion;
        this.mutability = mutability;
    }
    

    public void freezeIfNotFrozen() {
        mutability = Mutability.Immutable;
    }
    
    public void freeze() {
        mutability.ensureMutable();
        mutability = Mutability.Immutable;
    }
    
    @Override
    public RingRegion getRegion() {
        return ringRegion;
    }
    
    /**
     * Precondition - each node has a valid min/max which
     * implies that any children are in order
     * @param nodes
     */
    protected void verifyListOrder(List<? extends ChecksumNode> nodes) {
        for (int i = 0; i < nodes.size() - 1; i++) {
            ChecksumNode    n0;
            ChecksumNode    n1;
            
            n0 = nodes.get(i);
            n1 = nodes.get(i + 1);
            //if (n0.maxKey().compareTo(n1.minKey()) >= 0) {
            if (n0.getRegion().getEnd() >= n1.getRegion().getStart()) {
                System.err.printf("%s\n", n0);
                System.err.printf("%s\n", n1);
                System.err.printf("%s\t%s\n", 
                        Long.toString(n0.getRegion().getEnd()), 
                        Long.toString(n1.getRegion().getStart()));
                System.err.printf("%s\t%s\n", 
                        Long.toHexString(n0.getRegion().getEnd()), 
                        Long.toHexString(n1.getRegion().getStart()));
                throw new RuntimeException("Children out of order");
            }
        }
    }
    
    @Override
    public boolean matches(ChecksumNode other) {
        return getChecksum().equals(other.getChecksum());
    }
    
    @Override
    public String toString() {
        StringBuilder   sb;
        
        sb = new StringBuilder();
        toString(sb, 0);
        return sb.toString();
    }
    
    public void toString(StringBuilder sb, int depth) {
        sb.append(String.format("%s[%s]\n", StringUtil.replicate('\t', depth), getRegion()));
        for (ChecksumNode child : getChildren()) {
            child.toString(sb, depth + 1);
        }
    }
}

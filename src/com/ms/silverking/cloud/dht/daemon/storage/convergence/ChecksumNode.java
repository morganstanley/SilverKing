package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.Iterator;
import java.util.List;

import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.ring.RingRegion;

public interface ChecksumNode {
    public RingRegion getRegion();
    public ConvergenceChecksum getChecksum();
    public boolean matches(ChecksumNode other);
    public List<? extends ChecksumNode> getChildren();
    public void toString(StringBuilder sb, int depth);
    public ChecksumNode duplicate();
    public Iterator<KeyAndVersionChecksum> iterator();
    public int estimatedKeys();
    public ChecksumNode getNodeForRegion(RingRegion region);
}

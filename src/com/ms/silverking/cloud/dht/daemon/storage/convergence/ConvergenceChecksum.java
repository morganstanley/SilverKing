package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;

public class ConvergenceChecksum {
    private final long  msl;
    private final long  lsl;
    
    public ConvergenceChecksum(long msl, long lsl) {
        this.msl = msl;
        this.lsl = lsl;
    }
    
    public ConvergenceChecksum(KeyAndVersionChecksum kc) {
        this(kc.getKey().getMSL(), kc.getKey().getLSL() ^ kc.getVersionChecksum());
    }
    
    @Override
    public boolean equals(Object other) {
        ConvergenceChecksum oCC;
        
        oCC = (ConvergenceChecksum)other;
        return msl == oCC.msl && lsl == oCC.lsl;
    }
    
    @Override
    public int hashCode() {
        return (int)((msl >>> 32) ^ msl ^ (lsl >>> 32) ^ lsl);
    }
    
    public ConvergenceChecksum xor(ConvergenceChecksum other) {
        return new ConvergenceChecksum(msl ^ other.msl, lsl ^ other.lsl);
    }
    
    public String toString() {
        return Long.toHexString(msl) +":"+ Long.toHexString(lsl);
    }
}

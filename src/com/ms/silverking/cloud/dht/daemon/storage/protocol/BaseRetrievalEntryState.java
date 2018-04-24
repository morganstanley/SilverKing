package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.AbsMillisTimeSource;

public abstract class BaseRetrievalEntryState {
    protected static AbsMillisTimeSource  absMillisTimeSource;
    
    private long                nextTimeoutAbsMillis;
    private static final int    relTimeoutMillis = 100; // FUTURE - make configurable
    static final int            minRelTimeoutMillis = relTimeoutMillis;
    
    public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
        absMillisTimeSource = _absMillisTimeSource;
    }
    
    public BaseRetrievalEntryState() {
        nextTimeoutAbsMillis = absMillisTimeSource.absTimeMillis() + relTimeoutMillis;
    }
    
    public abstract IPAndPort getInitialReplica();
    public abstract IPAndPort currentReplica();
    public abstract IPAndPort nextReplica();
    public abstract boolean isComplete();
    public abstract boolean prevReplicaSameAsCurrent();
    
    protected void incrementReplicaTimeout() {
        nextTimeoutAbsMillis = absMillisTimeSource.absTimeMillis() + relTimeoutMillis;
    }
    
    public boolean hasTimedOut(long curTimeMillis) {
        return !isComplete() && curTimeMillis > nextTimeoutAbsMillis;
    }    
}

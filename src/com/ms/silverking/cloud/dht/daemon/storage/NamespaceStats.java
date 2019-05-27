package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.concurrent.atomic.AtomicLong;

public class NamespaceStats {
    private volatile int    totalKeys; // meta write lock is held
    private AtomicLong      bytesUncompressed;
    private AtomicLong      bytesCompressed;
    private volatile long    totalPuts; // write lock is held when updating puts
    private volatile long    totalInvalidations; // write lock is held when updating puts
    private AtomicLong      totalRetrievals; // only read lock is held; use atomic
    private volatile long    lastPutMillis;
    private volatile long    lastRetrievalMillis;

    public NamespaceStats() {
        bytesUncompressed = new AtomicLong();
        bytesCompressed = new AtomicLong();
        totalRetrievals = new AtomicLong();
    }
    
    public int getTotalKeys() {
        return totalKeys;
    }
    
    public long getBytesUncompressed() {
        return bytesUncompressed.get();
    }
    
    public long getBytesCompressed() {
        return bytesCompressed.get();
    }
    
    public void incTotalKeys() {
        ++totalKeys;
    }
    
    public void addBytes(int bytesUncompressed, int bytesCompressed) {
        this.bytesUncompressed.addAndGet(bytesUncompressed);
        this.bytesCompressed.addAndGet(bytesCompressed);
    }
    
    public void addPuts(int numPuts, int numInvalidations, long timeMillis) {
        totalPuts += numPuts;
        totalInvalidations += numInvalidations;
        lastPutMillis = timeMillis;
    }
    
    public void addRetrievals(int numRetrievals, long timeMillis) {
        totalRetrievals.addAndGet(numRetrievals);
        lastRetrievalMillis = timeMillis;
    }
    
    public long getTotalPuts() {
        return totalPuts;
    }
    
    public long getTotalInvalidations() {
        return totalInvalidations;
    }
    
    public long getTotalRetrievals() {
        return totalRetrievals.get();
    }
    
    public long getLastPutMillis() {
        return lastPutMillis;
    }
    
    public long getLastRetrievalMillis() {
        return lastRetrievalMillis;
    }
    
    public long getLastActivityMillis() {
        return Math.max(getLastPutMillis(), getLastRetrievalMillis());
    }
}
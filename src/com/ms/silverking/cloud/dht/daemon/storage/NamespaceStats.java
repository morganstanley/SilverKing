package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NamespaceStats {
    private AtomicInteger   totalKeys;
    private AtomicLong      bytesUncompressed;
    private AtomicLong      bytesCompressed;

    public NamespaceStats() {
        totalKeys = new AtomicInteger();
        bytesUncompressed = new AtomicLong();
        bytesCompressed = new AtomicLong();
    }
    
    public int getTotalKeys() {
        return totalKeys.get();
    }
    
    public long getBytesUncompressed() {
        return bytesUncompressed.get();
    }
    
    public long getBytesCompressed() {
        return bytesCompressed.get();
    }
    
    public void incTotalKeys() {
        totalKeys.incrementAndGet();
    }
    
    public void addBytes(int bytesUncompressed, int bytesCompressed) {
        this.bytesUncompressed.addAndGet(bytesUncompressed);
        this.bytesCompressed.addAndGet(bytesCompressed);
    }
}
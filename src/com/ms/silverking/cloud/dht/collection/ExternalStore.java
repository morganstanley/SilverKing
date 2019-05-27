package com.ms.silverking.cloud.dht.collection;


/**
 * Storage for full keys stored in a PartialKeyCuckoo store.
 */
public interface ExternalStore {
    public boolean entryMatches(int offset, long msl, long lsl);
    public long getMSL(int offset);
    public long getLSL(int offset);
}

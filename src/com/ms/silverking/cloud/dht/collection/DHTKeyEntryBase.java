package com.ms.silverking.cloud.dht.collection;

import com.ms.silverking.cloud.dht.common.DHTKey;

/**
 * A map entry from a DHTKey-based hash table. Pairs the DHTKey with the value that the key maps to in the hash table.
 */
abstract class DHTKeyEntryBase implements DHTKey {
    private final long  msl;
    private final long  lsl;
    
    DHTKeyEntryBase(long msl, long lsl) {
        this.msl = msl;
        this.lsl = lsl;
    }
    
    @Override
    public long getMSL() {
        return msl;
    }

    @Override
    public long getLSL() {
        return lsl;
    }
    
    @Override
    public int hashCode() {
        return (int)lsl;
    }
    
    @Override
    public boolean equals(Object o) {
        DHTKey   oKey;
        
        oKey = (DHTKey)o;
        return lsl == oKey.getLSL() && msl == oKey.getMSL();
    }    

    /**
     * Return the key from this entry.
     * @return the key of this entry
     */
    public DHTKey getKey() {
        return this;
    }
}
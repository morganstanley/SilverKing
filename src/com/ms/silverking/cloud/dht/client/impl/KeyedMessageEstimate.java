package com.ms.silverking.cloud.dht.client.impl;

public class KeyedMessageEstimate extends MessageEstimate {
    private int   numKeys;
    
    //protected StringBuilder   sb = new StringBuilder();
    
    public KeyedMessageEstimate(int numKeys) {
        this.numKeys = numKeys;
    }
    
    public KeyedMessageEstimate() {
        this(0);
    }
    
    public void addKeys(int delta) {
        numKeys += delta;
        //sb.append(" +k:"+ delta);
        assert numKeys >= 0;
    }
    
    public int getNumKeys() {
        return numKeys;
    }
    
    @Override
    public String toString() {
        return Integer.toString(numKeys);
    }

    public void add(KeyedMessageEstimate oEstimate) {
        numKeys += oEstimate.numKeys;
    }
}

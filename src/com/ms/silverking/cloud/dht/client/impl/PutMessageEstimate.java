package com.ms.silverking.cloud.dht.client.impl;

public class PutMessageEstimate extends KeyedMessageEstimate {
    private int numBytes;
    //public final RuntimeException  re;
    
    public PutMessageEstimate(int numKeys, int numBytes) {
        super(numKeys);
        this.numBytes = numBytes;
        /*
        try {
            throw new RuntimeException();
        } catch (RuntimeException re) {
            this.re = re;
        }
        */
    }

    public PutMessageEstimate() {
        this(0, 0);
    }
    
    public void addBytes(int delta) {
        numBytes += delta;
        //sb.append(" +b:"+ delta);
    }

    public void add(PutMessageEstimate oEstimate) {
        super.add(oEstimate);
        numBytes += oEstimate.numBytes;
    }
    
    public int getNumBytes() {
        return numBytes;
    }
    
    @Override
    public String toString() {
        return super.toString() +":"+ numBytes;// +"\t"+ sb.toString();
    }
}

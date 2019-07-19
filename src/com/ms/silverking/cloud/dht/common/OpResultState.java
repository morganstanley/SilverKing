package com.ms.silverking.cloud.dht.common;


/**
 * Result state for non-retrieval operations. 
 */
public class OpResultState {
    private final OpResultListener listener;
    private volatile OpResult    opResult;
    
    public OpResultState(OpResultListener listener) {
        this.opResult = OpResult.INCOMPLETE;
        this.listener = listener;
    }
    
    public OpResultListener getListener() {
        return listener;
    }
    
    public OpResult getOpResult() {
        return opResult;
    }
    
    public void setResult(OpResult opResult) {
        this.opResult = opResult;
    }
}

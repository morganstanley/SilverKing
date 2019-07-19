package com.ms.silverking.thread.lwt;

public class LWTPoolLoadStats {
    private double  curLoad;
    
    private static final double alpha = 0.05;
    
    public LWTPoolLoadStats() {
        curLoad = 0.0;
    }
    
    public double getLoad() {
        return curLoad;
    }
    
    public void addLoadSample(double load) {
        curLoad = curLoad * (1.0 - alpha) + load * alpha;
    }
}

package com.ms.silverking.numeric;

public final class LogBaseN {
    private final double    base;
    private final double    logBase;
    
    public static final LogBaseN  log2 = new LogBaseN(2.0);
    public static final LogBaseN  log10 = new LogBaseN(10.0);
    
    public LogBaseN(double base) {
        this.base = base;
        this.logBase = Math.log(base);
    }
    
    public double log(double x) {
        return Math.log(x) / logBase;
    }
    
    public String toString() {
        return "log "+ base;
    }
}

package com.ms.silverking.thread.lwt;

public class LWTPoolParameters {
    private final String    name;
    private final int       targetSize;
    private final int       maxSize;
    private final boolean   commonQueue;
    private final int       workUnit;
    
    private static final int        defaultTargetSize = 1;
    private static final int        defaultMaxSize = 1;
    private static final boolean    defaultCommonQueue = true;
    private static final int        defaultWorkUnit = 1;
    
    private LWTPoolParameters(String name, int targetSize, int maxSize, boolean commonQueue, int workUnit) {
        this.name = name;
        this.targetSize = targetSize;
        this.maxSize = maxSize;
        this.commonQueue = commonQueue;
        this.workUnit = workUnit;
    }
    
    public static LWTPoolParameters create(String name) {
        return new LWTPoolParameters(name, defaultTargetSize, defaultMaxSize, defaultCommonQueue, defaultWorkUnit);
    }
    
    public LWTPoolParameters targetSize(int targetSize) {
        return new LWTPoolParameters(name, targetSize, Math.max(maxSize, targetSize), commonQueue, workUnit);
    }
    
    public LWTPoolParameters maxSize(int maxSize) {
        return new LWTPoolParameters(name, targetSize, maxSize, commonQueue, workUnit);
    }
    
    public LWTPoolParameters commonQueue(boolean commonQueue) {
        return new LWTPoolParameters(name, targetSize, maxSize, commonQueue, workUnit);
    }
    
    public LWTPoolParameters workUnit(int workUnit) {
        return new LWTPoolParameters(name, targetSize, maxSize, commonQueue, workUnit);
    }
    
    public String getName() {
        return name;
    }

    public int getTargetSize() {
        return targetSize;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public boolean getCommonQueue() {
        return commonQueue;
    }

    public int getWorkUnit() {
        return workUnit;
    }
    
    
    
    @Override
    public String toString() {
        return name +":"+ targetSize +":"+ maxSize +":"+ commonQueue +":"+ workUnit;
    }
}

package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.id.UUIDBase;

public class EmbeddedSKConfiguration {
    private final String    dhtName;
    private final String    gridConfigName;
    private final String    ringName;
    private final int       replication;

    private static final String defaultInstanceNamePrefix = "SK.";
    private static final String defaultGridConfigNamePrefix = "GC_SK_";
    private static final String defaultRingNamePrefix = "ring.";
    private static final int    defaultReplication = 1;
    
    public EmbeddedSKConfiguration(String dhtName, String gridConfigName, String ringName, int replication) {
        this.dhtName = dhtName;
        this.gridConfigName = gridConfigName;
        this.ringName = ringName;
        this.replication = replication;
    }
    
    public EmbeddedSKConfiguration(String id, int replication) {
        this(defaultInstanceNamePrefix + id, defaultGridConfigNamePrefix + id, defaultRingNamePrefix + id, replication);
    }
    
    public EmbeddedSKConfiguration(String id) {
        this(defaultInstanceNamePrefix + id, defaultGridConfigNamePrefix + id, defaultRingNamePrefix + id, defaultReplication);
    }
    
    public EmbeddedSKConfiguration(int replication) {
        this(new UUIDBase(false).toString(), replication);
    }
    
    public EmbeddedSKConfiguration() {
        this(new UUIDBase(false).toString());
    }
    
    public String getDHTName() {
        return dhtName;
    }
    
    public String getGridConfigName() {
        return gridConfigName;
    }
    
    public String getRingName() {
        return ringName;
    }
    
    public int getReplication() {
        return replication;
    }
    
    public EmbeddedSKConfiguration dhtName(String dhtName) {
        System.out.printf("dhtName %s \n", dhtName);
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication);
    }
    
    public EmbeddedSKConfiguration gridConfigName(String gridConfigName) {
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication);
    }
    
    public EmbeddedSKConfiguration ringName(String ringName) {
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication);
    }
    
    public EmbeddedSKConfiguration replication(int replication) {
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication);
    }
}

package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.id.UUIDBase;

public class EmbeddedSKConfiguration {
    private final String                dhtName;
    private final String                gridConfigName;
    private final String                ringName;
    private final int                   replication;
    private final NamespaceOptionsMode namespaceOptionsMode;

    private static final String defaultInstanceNamePrefix = "SK.";
    private static final String defaultGridConfigNamePrefix = "GC_SK_";
    private static final String defaultRingNamePrefix = "ring.";
    private static final int    defaultReplication = 1;

    public EmbeddedSKConfiguration(String dhtName, String gridConfigName, String ringName, int replication,
                                   NamespaceOptionsMode namespaceOptionsMode) {
        this.dhtName = dhtName;
        this.gridConfigName = gridConfigName;
        this.ringName = ringName;
        this.replication = replication;
        this.namespaceOptionsMode = namespaceOptionsMode;
    }

    public EmbeddedSKConfiguration(String id, int replication) {
        this(defaultInstanceNamePrefix + id, defaultGridConfigNamePrefix + id, defaultRingNamePrefix + id, replication,
                DHTConfiguration.defaultNamespaceOptionsMode);
    }

    public EmbeddedSKConfiguration(String id) {
        this(defaultInstanceNamePrefix + id, defaultGridConfigNamePrefix + id, defaultRingNamePrefix + id,
                defaultReplication, DHTConfiguration.defaultNamespaceOptionsMode);
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

    public NamespaceOptionsMode getNamespaceOptionsMode() {
        return namespaceOptionsMode;
    }

    public EmbeddedSKConfiguration dhtName(String dhtName) {
        System.out.printf("dhtName %s \n", dhtName);
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication, namespaceOptionsMode);
    }

    public EmbeddedSKConfiguration gridConfigName(String gridConfigName) {
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication, namespaceOptionsMode);
    }

    public EmbeddedSKConfiguration ringName(String ringName) {
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication, namespaceOptionsMode);
    }

    public EmbeddedSKConfiguration replication(int replication) {
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication, namespaceOptionsMode);
    }

    public EmbeddedSKConfiguration namespaceOptionsMode(NamespaceOptionsMode namespaceOptionsMode) {
        return new EmbeddedSKConfiguration(dhtName, gridConfigName, ringName, replication, namespaceOptionsMode);
    }
}

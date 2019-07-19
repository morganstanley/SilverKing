package com.ms.silverking.cloud.toporing.meta;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;

public class MetaPaths extends com.ms.silverking.cloud.meta.MetaPaths {
    // configuration instance paths
    private final String  weightsPath;
    //private final String  replicationPath;
    private final String  instancePath;
    private final String  configPath;
    private final String storagePolicyGroupPath;
        
    // dht global base directories
    public static final String  ringsGlobalBase = cloudGlobalBase +"/rings";
    public static final String  weightsBase = ringsGlobalBase +"/weights";
    //public static final String  replicationBase = ringsGlobalBase +"/replication";
    public static final String  instancesBase = ringsGlobalBase +"/instances";
    public static final String  storagePolicyBase = ringsGlobalBase +"/storagePolicyGroups";
        
    private static final String  configElement = "config";
    
    public MetaPaths(NamedRingConfiguration namedRingConfig) {
        super(namedRingConfig.getRingConfiguration().getCloudConfiguration());    
        
        RingConfiguration               ringConfig;
        ImmutableList.Builder<String>   listBuilder;
        
        ringConfig = namedRingConfig.getRingConfiguration();
        listBuilder = ImmutableList.builder();
        if (ringConfig.getWeightSpecsName() != null) {
            weightsPath = weightsBase +"/"+ ringConfig.getWeightSpecsName();
            listBuilder.add(weightsPath);
        } else {
            weightsPath = null;
        }
        /*
        if (ringConfig.getReplicationSpecsName() != null) {
            replicationPath = replicationBase +"/"+ ringConfig.getReplicationSpecsName();
            listBuilder.add(replicationPath);
        } else {
            replicationPath = null;
        }
        */
        
        if (namedRingConfig.getRingConfiguration().getStoragePolicyGroupName() != null) {
            storagePolicyGroupPath = storagePolicyBase +"/"+ namedRingConfig.getRingConfiguration().getStoragePolicyGroupName();
            listBuilder.add(storagePolicyGroupPath);
        } else {
            storagePolicyGroupPath = null;
        }
        
        if (namedRingConfig.getRingName() != null) {
            instancePath = instancesBase +"/"+ namedRingConfig.getRingName();
            configPath = instancePath +"/"+ configElement;
            //configPath = instancePath +"/config/" + ringConfig.getVersion();
            listBuilder.add(configPath);
        } else {
            instancePath = null;
            configPath = null;
        }
        pathList = listBuilder.build();      
    }
    
    public String getWeightsPath() {
        return weightsPath;
    }

    public String getConfigPath() {
        return configPath;
    }

    public static String getRingConfigPath(String ringName) {
        return instancesBase +"/"+ ringName +"/"+ configElement;
    }
    
    public static String getRingConfigInstancePath(String ringName, long configVersion) {
        return getConfigInstancePath(getRingConfigPath(ringName), configVersion);
    }
    
    private static String getConfigInstancePath(String configPath, long configVersion) {
        return ZooKeeperExtended.padVersionPath(configPath, configVersion) +"/instance";
    }
    
    public String getConfigInstancePath(long configVersion) {
        return getConfigInstancePath(configPath, configVersion);
    }
    
    public String getRingInstancePath(long configVersion, long instanceVersion) {
        return getRingInstancePath(getConfigInstancePath(configVersion), instanceVersion);
    }
    
    public String getRingInstancePath(String configInstancePath, long instanceVersion) {
        return ZooKeeperExtended.padVersionPath(configInstancePath, instanceVersion);
    }
    
    public String getStoragePolicyGroupPath() {
        return storagePolicyGroupPath;
    }
}

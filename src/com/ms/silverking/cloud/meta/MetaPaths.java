package com.ms.silverking.cloud.meta;

import com.google.common.collect.ImmutableList;

/**
 * Global and configuration specific cloud meta paths. 
 */
public class MetaPaths extends MetaPathsBase {
    // instance paths
    private final String topologyPath;
    private final String exclusionsPath;
    private final String hostGroupPath;
    
    // cloud global base directories
    public static final String  topologiesBase = cloudGlobalBase +"/topologies";
    public static final String  exclusionsBase = cloudGlobalBase +"/exclusions";
    public static final String  hostGroupBase = cloudGlobalBase +"/hostGroupTables";
    
    public MetaPaths(CloudConfiguration cloudConfig) {
        ImmutableList.Builder<String>   listBuilder;
        
        listBuilder = ImmutableList.builder();
        if (cloudConfig.getTopologyName() != null) {
            topologyPath = topologiesBase +"/"+ cloudConfig.getTopologyName();
            listBuilder.add(topologyPath);
        } else {
            topologyPath = null;
        }
        if (cloudConfig.getExclusionSpecsName() != null) {
            exclusionsPath = exclusionsBase +"/"+ cloudConfig.getExclusionSpecsName();
            listBuilder.add(exclusionsPath);
        } else {
            exclusionsPath = null;
        }
        if (cloudConfig.getHostGroupTableName() != null) {
            hostGroupPath = hostGroupBase +"/"+ cloudConfig.getHostGroupTableName();
            listBuilder.add(hostGroupPath);
        } else {
            hostGroupPath = null;
        }
        pathList = listBuilder.build();
    }
    
    public String getTopologyPath() {
        return topologyPath;
    }

    public String getExclusionsPath() {
        return exclusionsPath;
    }
    
    public String getHostGroupPath() {
        return hostGroupPath;
    }    
}

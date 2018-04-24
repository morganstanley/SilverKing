package com.ms.silverking.cloud.dht.meta;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.meta.MetaPathsBase;

public class MetaPaths extends MetaPathsBase {
    // configuration instance paths
    private final String  instancePath;
    private final String  instanceConfigPath;
    private final String  instanceSKFSConfigPath;
    private final String  instanceNSLinkPath;
    private final String  instanceSuspectsPath;
    private final String  instanceExclusionsPath;
    private final String  instanceCommandsPath;
    private final String  instanceRingStatePath;
    private final String  instanceDaemonStatePath;
    private final String  instanceNodeInfoPath;
    private final String  passiveNodesPath;
        
    // dht global base directories
    public static final String  dhtGlobalBase = cloudGlobalBase +"/dht";
    public static final String  instancesBase = dhtGlobalBase +"/instances";
    public static final String  passiveNodesBase = dhtGlobalBase +"/passiveNodes";
    public static final String  classVarsBase = dhtGlobalBase +"/classVars";
    
    private static final String configNodeName = "config";
    private static final String nsLinkNodeName = "nsLink";
    private static final String suspectsNodeName = "suspects";
    private static final String nodeInfoNodeName = "nodeInfo";
    private static final String exclusionsNodeName = "exclusions";
    private static final String commandsNodeName = "commands";
    private static final String ringStateNodeName = "ringState";
    private static final String daemonStateNodeName = "daemon";
    private static final String skfsConfigNodeName = "skfsConfig";
        
    private static final String curRingAndVersionPairNodeName = "curRingAndVersionPair";
    private static final String targetRingAndVersionPairNodeName = "targetRingAndVersionPair";
    private static final String masterRingAndVersionPairNodeName = "masterRingAndVersionPair";
    
    /*
    public final String         dhtInfoBase;
    public final String         nodesBase;
    public final String         znodesBase;
    public final String         bootNodesBase;
    public final String         blacklistedNodesBase;
    public final String         namespacesBase;
    public final String         startTokenPath;
    public final String         bootIdPath;
    public final String         nodeLocalBasePathPath;
    public final String         classBase;
    */
    
    public MetaPaths(NamedDHTConfiguration dhtConfig) {
        ImmutableList.Builder<String>   listBuilder;
        
        listBuilder = ImmutableList.builder();
        
        if (dhtConfig == null) {
            dhtConfig = new NamedDHTConfiguration(null, DHTConfiguration.emptyTemplate);
        }
        instancePath = instancesBase +"/"+ dhtConfig.getDHTName();
        listBuilder.add(instancePath);
        instanceConfigPath = instancePath +"/"+ configNodeName;
        listBuilder.add(instanceConfigPath);
        instanceSKFSConfigPath = instancePath +"/"+ skfsConfigNodeName;
        listBuilder.add(instanceSKFSConfigPath);
        instanceNSLinkPath = instancePath +"/"+ nsLinkNodeName;
        listBuilder.add(instanceNSLinkPath);
        instanceSuspectsPath = instancePath +"/"+ suspectsNodeName;
        listBuilder.add(instanceSuspectsPath);
        instanceNodeInfoPath = instancePath +"/"+ nodeInfoNodeName;
        listBuilder.add(instanceNodeInfoPath);
        instanceExclusionsPath = instancePath +"/"+ exclusionsNodeName;
        listBuilder.add(instanceExclusionsPath);
        instanceCommandsPath = instancePath +"/"+ commandsNodeName;
        listBuilder.add(instanceCommandsPath);
        instanceRingStatePath = instancePath +"/"+ ringStateNodeName;
        listBuilder.add(instanceRingStatePath);
        instanceDaemonStatePath = instancePath +"/"+ daemonStateNodeName;
        listBuilder.add(instanceDaemonStatePath);
        
        if (dhtConfig.getDHTConfig() != null && dhtConfig.getDHTConfig().getPassiveNodeHostGroups() != null) {
            passiveNodesPath = passiveNodesBase +"/"+ dhtConfig.getDHTConfig().getPassiveNodeHostGroups();
            listBuilder.add(passiveNodesPath);
        } else {
            passiveNodesPath = null;
        }
        /*
        if (passiveNodes != null) {
            passiveNodesPath = passiveNodesBase +"/"+ passiveNodes;
            listBuilder.add(passiveNodesPath);
        } else {
            passiveNodesPath = null;
        }
        */
        listBuilder.add(classVarsBase);
        
        pathList = listBuilder.build();
      
        /*
        dhtInfoBase = dhtGlobalBase +"/info/"+ dhtName;
        nodesBase = dhtBase +"/nodes";
        znodesBase = dhtBase +"/znodes";
        bootNodesBase = dhtBase +"/bootNodes";
        blacklistedNodesBase = dhtBase +"/blacklistedNodes";
        namespacesBase = dhtBase +"/namespaces";
        startTokenPath = dhtBase +"/startToken";
        bootIdPath = dhtBase +"/bootId";
        nodeLocalBasePathPath = dhtBase +"/nodeBasePath";
        classBase = dhtBase +"/nodeClasses";
        */
    }
    
    public static String getInstancePath(String dhtName) {
        return instancesBase +"/"+ dhtName;
    }
    
    public static String getInstanceNSLinkPath(String dhtName) {
        return getInstancePath(dhtName) +"/"+ nsLinkNodeName;
    }
    
    public static String getInstanceSuspectsPath(String dhtName) {
        return getInstancePath(dhtName) +"/"+ suspectsNodeName;
    }
    
    public static String getInstanceNodeInfoPath(String dhtName) {
        return getInstancePath(dhtName) +"/"+ nodeInfoNodeName;
    }
    
    public static String getInstanceExclusionsPath(String dhtName) {
        return getInstancePath(dhtName) +"/"+ exclusionsNodeName;
    }
    
    public static String getInstanceCommandsPath(String dhtName) {
        return getInstancePath(dhtName) +"/"+ commandsNodeName;
    }
    
    public static String getInstanceRingStatePath(String dhtName) {
        return getInstancePath(dhtName) +"/"+ ringStateNodeName;
    }
    
    public static String getInstanceDaemonStatePath(String dhtName) {
        return getInstancePath(dhtName) +"/"+ daemonStateNodeName;
    }
    
    public static String getInstanceCurRingAndVersionPairPath(String dhtName) {
        return getInstanceRingStatePath(dhtName) +"/"+ curRingAndVersionPairNodeName;
    }
    
    public static String getInstanceTargetRingAndVersionPairPath(String dhtName) {
        return getInstanceRingStatePath(dhtName) +"/"+ targetRingAndVersionPairNodeName;
    }
    
    public static String getInstanceMasterRingAndVersionPairPath(String dhtName) {
        return getInstanceRingStatePath(dhtName) +"/"+ masterRingAndVersionPairNodeName;
    }
    
    public String getInstancePath() {
        return instancePath;
    }

    public String getInstanceConfigPath() {
        return instanceConfigPath;
    }
    
    public String getInstanceSKFSConfigPath() {
        return instanceSKFSConfigPath;
    }
    
    public String getInstanceNSLinkPath() {
        return instanceNSLinkPath;
    }
    
    public String getInstanceSuspectsPath() {
        return instanceSuspectsPath;
    }
    
    public String getInstanceExclusionsPath() {
        return instanceExclusionsPath;
    }
    
    public String getInstanceCommandsPath() {
        return instanceCommandsPath;
    }
    
    public String getInstanceRingStatePath() {
        return instanceRingStatePath;
    }
    
    public String getInstanceDaemonStatePath() {
        return instanceDaemonStatePath;
    }
    
    public String getInstanceNodeInfoPath() {
        return instanceNodeInfoPath;
    }
    
    public String getPassiveNodesPath() {
        return passiveNodesPath;
    }
    
    public String getClassVarsBasePath() {
        return classVarsBase;
    }    
}

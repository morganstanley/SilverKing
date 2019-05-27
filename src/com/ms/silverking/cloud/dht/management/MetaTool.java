package com.ms.silverking.cloud.dht.management;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.meta.ClassVarsZK;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.NamedDHTConfiguration;
import com.ms.silverking.cloud.management.MetaToolBase;
import com.ms.silverking.cloud.management.MetaToolModule;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;

public class MetaTool extends MetaToolBase {
    private enum Tool {DHTConfiguration,PassiveNodes,ClassVars};
    
    public MetaTool() {
    }
    
    private static MetaToolModule getModule(Tool tool, MetaClient metaClient) throws KeeperException {
        switch (tool) {
        case DHTConfiguration: return new DHTConfigurationZK(metaClient);
        case PassiveNodes: return new ServerSetExtensionZK(metaClient, metaClient.getMetaPaths().getPassiveNodesPath());
        case ClassVars: return new ClassVarsZK(metaClient);
        default: throw new RuntimeException("panic");
        }
    }
    
    private static NamedDHTConfiguration namedDHTConfigurationFor(Tool tool, String name) {
        switch (tool) {
        case DHTConfiguration:
            return new NamedDHTConfiguration(name, null);
        case PassiveNodes: 
            DHTConfiguration    dhtConfig;
            
            dhtConfig = DHTConfiguration.forPassiveNodes(name);
            return new NamedDHTConfiguration(null, dhtConfig);
        case ClassVars:
            return new NamedDHTConfiguration(null, DHTConfiguration.emptyTemplate);
        default: throw new RuntimeException("panic");
        }
    }

    @Override
    protected void doWork(MetaToolOptions options) throws IOException, KeeperException {
        MetaClient      mc;
        Tool            tool;
        ZooKeeperConfig zkConfig;
        
        tool = Tool.valueOf(options.tool);
        zkConfig = new ZooKeeperConfig(options.zkConfig);
        mc = new MetaClient(namedDHTConfigurationFor(tool, options.name), zkConfig);
        doWork(options, new MetaToolWorker(getModule(tool, mc)));
    }
    
    public static void main(String[] args) {
        new MetaTool().runTool(args);
    }
}

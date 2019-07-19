package com.ms.silverking.cloud.management;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.HostGroupTableZK;
import com.ms.silverking.cloud.meta.MetaClient;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;

public class MetaTool extends MetaToolBase {
    private enum Tool {Topology, Exclusions, PassiveNodes, HostGroupTables};
    
    public MetaTool() {
    }
    
    private static MetaToolModule getModule(Tool tool, MetaClient metaClient) throws KeeperException {
        switch (tool) {
        case Topology: return new TopologyZK(metaClient);
        case Exclusions: return new ServerSetExtensionZK(metaClient, metaClient.getMetaPaths().getExclusionsPath());
        //case PassiveNodes: return new ServerSetExtensionZK(metaClient, metaClient.getMetaPaths().getPassiveNodesPath());
        case PassiveNodes: return null;
        case HostGroupTables: return new HostGroupTableZK(metaClient);
        default: throw new RuntimeException("panic");
        }
    }
    
    private static CloudConfiguration cloudConfigurationFor(Tool tool, String name) {
        switch (tool) {
        case Topology: return CloudConfiguration.emptyTemplate.topologyName(name);
        case Exclusions: return CloudConfiguration.emptyTemplate.exclusionSpecsName(name);
        case HostGroupTables: return CloudConfiguration.emptyTemplate.hostGroupTableName(name);
        default: throw new RuntimeException("panic");
        }
    }
    
    @Override
    protected void doWork(MetaToolOptions options) throws IOException, KeeperException {
        MetaClient      mc;
        Tool            tool;
        
        tool = Tool.valueOf(options.tool);
        mc = new MetaClient(cloudConfigurationFor(tool, options.name), new ZooKeeperConfig(options.zkConfig));
        doWork(options, new MetaToolWorker(getModule(tool, mc)));
    }
    
    public static void main(String[] args) {
        new MetaTool().runTool(args);
    }
}

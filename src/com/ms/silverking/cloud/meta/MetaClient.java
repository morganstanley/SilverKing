package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyParser;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.log.Log;

public class MetaClient extends MetaClientBase<MetaPaths> implements Watcher {
	private final CloudConfiguration	cloudConfig;
	
    public MetaClient(CloudConfiguration cloudConfig, ZooKeeperConfig zkConfig, Watcher watcher) throws IOException, KeeperException {
        super(new MetaPaths(cloudConfig), zkConfig, watcher);
        this.cloudConfig = cloudConfig;
    }

    public MetaClient(CloudConfiguration cloudConfig, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
        this(cloudConfig, zkConfig, null);
    }
    
    public CloudConfiguration getCloudConfiguration() {
    	return cloudConfig;
    }
    
    public void test() throws Exception {
        Topology    topology;
        
        topology = TopologyParser.parse(new File("c:/tmp/topo2.txt"));
        System.out.println(topology);
    }
    
    // for unit testing only
    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                System.out.println("args: <cloudConfiguration> <ensemble>");
            } else {
                MetaClient     mc;
                ZooKeeperConfig zc;
                CloudConfiguration    cloudConfig;
                
                cloudConfig = CloudConfiguration.parse(args[0]);
                zc = new ZooKeeperConfig(args[1]);
                mc = new MetaClient(cloudConfig, zc);
                mc.test();
            }
        } catch (Exception e) {
            Log.logErrorWarning(e);
        }
    }
}

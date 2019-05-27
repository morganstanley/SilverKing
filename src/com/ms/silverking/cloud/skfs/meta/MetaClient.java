package com.ms.silverking.cloud.skfs.meta;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;

public class MetaClient extends MetaClientBase<MetaPaths> {
    private final String    skfsConfigName;
    
    public MetaClient(String skfsConfigName, ZooKeeperConfig zkConfig, Watcher watcher) throws IOException, KeeperException {
        super(new MetaPaths(skfsConfigName), zkConfig, watcher);
        this.skfsConfigName = skfsConfigName;
    }

    public MetaClient(String skfsConfigName, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
        this(skfsConfigName, zkConfig, null);
    }
    
    public MetaClient(SKGridConfiguration skGridConfig) throws IOException, KeeperException {
    	this(skGridConfig.getSKFSConfigName(), skGridConfig.getClientDHTConfiguration().getZKConfig());
    }
    
    public String getSKFSConfigName() {
        return skfsConfigName;
    }
    
    public String getSKFSConfig() throws KeeperException {
        String  def;
        //long    version;
        String  latestPath;
        //long    zxid;
        ZooKeeperExtended   zk;
        
        zk = getZooKeeper();
        latestPath = zk.getLatestVersionPath(getMetaPaths().getConfigPath());
        //version = zk.getLatestVersionFromPath(latestPath);
        def = zk.getString(latestPath);
        //zxid = zk.getStat(latestPath).getMzxid();
        //System.out.printf("\tzkid %x\n", zkid);
        return def;
    }
}

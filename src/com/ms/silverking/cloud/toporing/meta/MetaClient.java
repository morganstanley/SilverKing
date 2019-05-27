package com.ms.silverking.cloud.toporing.meta;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;

public class MetaClient extends MetaClientBase<MetaPaths> implements Watcher {
    private CloudConfiguration      cloudConfiguration;
    
    private MetaClient(MetaPaths mp, CloudConfiguration cloudConfiguration, ZooKeeperConfig zkConfig, Watcher watcher) throws IOException, KeeperException {
        super(mp, zkConfig, watcher);
        this.cloudConfiguration = cloudConfiguration;
    }
    
    //private MetaClient(MetaPaths mp, CloudConfiguration cloudConfiguration, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
    //    this(mp, cloudConfiguration, zkConfig, null);
    //}
    
    public MetaClient(NamedRingConfiguration ringConfig, ZooKeeperConfig zkConfig, Watcher watcher) throws IOException, KeeperException {
        this(new MetaPaths(ringConfig), ringConfig.getRingConfiguration().getCloudConfiguration(), zkConfig, watcher);
    }

    public MetaClient(NamedRingConfiguration ringConfig, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
        this(ringConfig, zkConfig, null);
    }
    
    public static MetaClient createMetaClient(String ringName, long ringVersion, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
        MetaClient	_mc;
        NamedRingConfiguration	ringConfig;
        
        _mc = new MetaClient(new NamedRingConfiguration(ringName, RingConfiguration.emptyTemplate), zkConfig);
        ringConfig = new NamedRingConfiguration(ringName, new RingConfigurationZK(_mc).readFromZK(ringVersion, null));
        return new com.ms.silverking.cloud.toporing.meta.MetaClient(ringConfig, zkConfig);
    }
    
    public com.ms.silverking.cloud.meta.MetaClient createCloudMC() throws KeeperException, IOException {
        return new com.ms.silverking.cloud.meta.MetaClient(cloudConfiguration, getZooKeeper().getZKConfig());
    }
    
    public String createConfigInstancePath(long configVersion) throws KeeperException {
        String  path;
        
        path = metaPaths.getConfigInstancePath(configVersion);
        getZooKeeper().createAllNodes(path);
        return getZooKeeper().createString(path +"/", "", CreateMode.PERSISTENT_SEQUENTIAL);
    }
    
    public String getLatestConfigInstancePath(long configVersion) throws KeeperException {
        String  path;
        long    latestVersion;
        
        path = metaPaths.getConfigInstancePath(configVersion);
        getZooKeeper().createAllNodes(path);
        latestVersion = getZooKeeper().getLatestVersion(path);
        if (latestVersion >= 0) {
            return path +"/"+ ZooKeeperExtended.padVersion(latestVersion);
        } else {
            return null;
        }
    }
    
    public long getLatestConfigInstanceVersion(long configVersion) throws KeeperException {
        String  path;
        long    latestVersion;
        
        path = metaPaths.getConfigInstancePath(configVersion);
        getZooKeeper().createAllNodes(path);
        latestVersion = getZooKeeper().getLatestVersion(path);
        return latestVersion;
    }
}

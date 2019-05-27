package com.ms.silverking.cloud.dht.meta;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.io.StreamParser;

public class DHTSKFSConfigurationZK extends MetaToolModuleBase<DHTSKFSConfiguration,MetaPaths> {
    public DHTSKFSConfigurationZK(MetaClient mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getInstanceSKFSConfigPath());
    }
    
    @Override
    public DHTSKFSConfiguration readFromFile(File file, long version) throws IOException {
        return DHTSKFSConfiguration.parse(StreamParser.parseLine(file), version);
    }

    @Override
    public DHTSKFSConfiguration readFromZK(long version, MetaToolOptions options) throws KeeperException {
        System.out.println(zk.getString(getVBase(version)));
        return DHTSKFSConfiguration.parse(zk.getString(getVBase(version)), version);
    }
    
    @Override
    public void writeToFile(File file, DHTSKFSConfiguration instance) throws IOException {
        throw new RuntimeException("writeToFile not implemented for WeightSpecifications");
    }

    @Override
    public String writeToZK(DHTSKFSConfiguration dhtSKFSConfig, MetaToolOptions options) throws IOException, KeeperException {
        String  path;
        
        path = zk.createString(base +"/" , dhtSKFSConfig.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
        return path;
    }    
}

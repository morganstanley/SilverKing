package com.ms.silverking.cloud.dht.meta;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.io.StreamParser;

public class DHTConfigurationZK extends MetaToolModuleBase<DHTConfiguration,MetaPaths> {
    public DHTConfigurationZK(MetaClient mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getInstanceConfigPath());
    }
    
    @Override
    public DHTConfiguration readFromFile(File file, long version) throws IOException {
        return DHTConfiguration.parse(StreamParser.parseLine(file), version);
    }

    @Override
    public DHTConfiguration readFromZK(long version, MetaToolOptions options) throws KeeperException {
        System.out.println(zk.getString(getVBase(version)));
        return DHTConfiguration.parse(zk.getString(getVBase(version)), version);
    }
    
    @Override
    public void writeToFile(File file, DHTConfiguration instance) throws IOException {
        throw new RuntimeException("writeToFile not implemented for WeightSpecifications");
    }

    @Override
    public String writeToZK(DHTConfiguration dhtConfig, MetaToolOptions options) throws IOException, KeeperException {
        String  path;
        
        path = zk.createString(base +"/" , dhtConfig.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
        return path;
    }    
}

package com.ms.silverking.cloud.toporing.meta;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.io.StreamParser;

public class RingConfigurationZK extends MetaToolModuleBase<RingConfiguration,MetaPaths> {
    public RingConfigurationZK(MetaClient mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getConfigPath());
    }
    
    @Override
    public RingConfiguration readFromFile(File file, long version) throws IOException {
        return RingConfiguration.parse(StreamParser.parseLine(file), version);
    }

    @Override
    public RingConfiguration readFromZK(long version, MetaToolOptions options) throws KeeperException {
        return RingConfiguration.parse(zk.getString(getVBase(version)), version);
    }
    
    @Override
    public void writeToFile(File file, RingConfiguration instance) throws IOException {
        throw new RuntimeException("writeToFile not implemented for WeightSpecifications");
    }

    @Override
    public String writeToZK(RingConfiguration ringConfig, MetaToolOptions options) throws IOException, KeeperException {
        String  path;
        
        path = zk.createString(base +"/" , ringConfig.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
        return path;
    }    
}

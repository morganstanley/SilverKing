package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;

public class HostGroupTableZK extends MetaToolModuleBase<HostGroupTable,MetaPaths> {
    public HostGroupTableZK(MetaClient mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getHostGroupPath());
    }
    
    @Override
    public HostGroupTable readFromFile(File file, long version) throws IOException {
        return HostGroupTable.parse(file, version);
    }

    @Override
    public HostGroupTable readFromZK(long version, MetaToolOptions options) throws KeeperException {
    	String	base;
    	String	vBase;
    	
    	base = getBase();
    	if (version < 0) {
    		version = zk.getLatestVersion(base);
    	}
    	vBase = getVBase(version);
        return HostGroupTable.parse(zk.getString(vBase), version);
    }
    
    @Override
    public void writeToFile(File file, HostGroupTable instance) throws IOException {
        throw new RuntimeException("writeToFile not implemented for HostGroupTable");
    }

    @Override
    public String writeToZK(HostGroupTable hostGroupTable, MetaToolOptions options) throws IOException, KeeperException {
        String  path;
        
        path = zk.createString(base +"/" , hostGroupTable.toString(), CreateMode.PERSISTENT_SEQUENTIAL);
        return path;
    }    
}

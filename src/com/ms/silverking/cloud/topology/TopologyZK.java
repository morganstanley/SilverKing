package com.ms.silverking.cloud.topology;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.meta.MetaClient;
import com.ms.silverking.cloud.meta.MetaPaths;

/**
 * Writes/reads topologies to/from Zookeeper.
 */
public class TopologyZK extends MetaToolModuleBase<Topology,MetaPaths> {
    public TopologyZK(MetaClient mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getTopologyPath());
    }
    
    public Topology readTopologyAsBlob(long version) throws KeeperException, TopologyParseException {
        String          vPath;
        String          def;
        
        vPath = getVersionPath(version);
        def = zk.getString(vPath);
        try {
            return TopologyParser.parseVersioned(def, version);
        } catch (IOException ioe) {
            throw new TopologyParseException(ioe);
        }
    }
    
    /////////////////    

    @Override
    public Topology readFromFile(File file, long version) throws IOException {
        return TopologyParser.parseVersioned(file, version);
    }

    @Override
    public Topology readFromZK(long version, MetaToolOptions options) throws KeeperException {
        try {
            return readTopologyAsBlob(version);
        } catch (TopologyParseException tpe) {
            throw new RuntimeException(tpe);
        }
    }

    @Override
    public void writeToFile(File file, Topology instance) throws IOException {
        throw new RuntimeException("writeToFile not yet implemented for TopologyZK");
    }

    @Override
    public String writeToZK(Topology topology, MetaToolOptions options) throws IOException, KeeperException {
        zk.createString(base +"/" , topology.toStructuredString(), CreateMode.PERSISTENT_SEQUENTIAL);
        return null;
    }
}

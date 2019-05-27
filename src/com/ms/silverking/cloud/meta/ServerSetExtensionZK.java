package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.io.IOUtil;

public class ServerSetExtensionZK<M extends MetaPaths> extends MetaToolModuleBase<ServerSet,M> {
    private static final char   delimiterChar = '\n';
    private static final String delimiterString = "" + delimiterChar;
    
    public ServerSetExtensionZK(MetaClientBase<M> mc, String base) throws KeeperException {
        super(mc, base);
    }
    
    @Override
    public ServerSet readFromFile(File file, long version) throws IOException {
        return ServerSet.parse(file, version);
    }

    @Override
    public ServerSet readFromZK(long version, MetaToolOptions options) throws KeeperException {
        String          vBase;
        //List<String>    nodes;
        String[]    nodes;

        vBase = getVBase(version);
        //nodes = zk.getChildren(vBase);
        nodes = zk.getString(vBase).split(delimiterString);
        return new ServerSet(ImmutableSet.copyOf(nodes), version);
    }
    
    @Override
    public void writeToFile(File file, ServerSet serverSet) throws IOException {
        IOUtil.writeAsLines(file, serverSet.getServers());
    }

    @Override
    public String writeToZK(ServerSet serverSet, MetaToolOptions options) throws IOException, KeeperException {
        String  vBase;
        String  zkVal;
        
        zkVal = CollectionUtil.toString(serverSet.getServers(), "", "", delimiterChar, "");
        vBase = zk.createString(base +"/" , zkVal, CreateMode.PERSISTENT_SEQUENTIAL);
        /*
        for (String entity : serverSet.getServers()) {
            //System.out.println(vBase +"/"+ entity);
            zk.createString(vBase +"/"+ entity, entity);
        }
        */
        return null;
    }

	public ServerSet readLatestFromZK() throws KeeperException {
		return readLatestFromZK(null);
	}
	
	public ServerSet readLatestFromZK(MetaToolOptions options) throws KeeperException {
        long            version;

        version = zk.getLatestVersion(base);
        return readFromZK(version, options);
	}
}

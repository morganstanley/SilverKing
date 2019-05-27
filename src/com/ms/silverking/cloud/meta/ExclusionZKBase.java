package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.io.IOUtil;

/**
 * Base functionality for both cloud-level, and dht instance specific ExclusionSets
 */
public abstract class ExclusionZKBase<M extends MetaPathsBase> extends MetaToolModuleBase<ExclusionSet,M> {
	private final String	exclusionsPath;
	
    private static final char   delimiterChar = '\n';
    private static final String delimiterString = "" + delimiterChar;
    
    public ExclusionZKBase(MetaClientBase<M> mc, String exclusionsPath) throws KeeperException {
        super(mc, exclusionsPath);
        this.exclusionsPath = exclusionsPath;
    }
    
    public String getExclusionsPath() {
    	return exclusionsPath;
    }
    
    public String getLatestZKPath() throws KeeperException {
        long            version;
        ZooKeeperExtended   _zk;
        
        _zk = mc.getZooKeeper();
        version = _zk.getLatestVersion(exclusionsPath);
        if (version < 0) {
        	return null;
        } else {
        	return getVBase(version);
        }
    }
    
    @Override
    public ExclusionSet readFromFile(File file, long version) throws IOException {
        return new ExclusionSet(ServerSet.parse(file, version));
    }

    @Override
    public ExclusionSet readFromZK(long version, MetaToolOptions options) throws KeeperException {
        String          vBase;
        //List<String>    nodes;
        String[]        nodes;
        Stat			stat;
        ZooKeeperExtended   _zk;
        
        _zk = mc.getZooKeeper();
        if (version == VersionedDefinition.NO_VERSION) {
        	version = _zk.getLatestVersion(base);
        }
        vBase = getVBase(version);
        //nodes = zk.getChildren(vBase);
        stat = new Stat();
        nodes = _zk.getString(vBase, null, stat).split("\n");
        return new ExclusionSet(ImmutableSet.copyOf(nodes), version, stat.getMzxid());
    }
    
    public ExclusionSet readLatestFromZK() throws KeeperException {
        return readLatestFromZK(null);
    }
    
    private Set<String> readNodesAsSet(String path, Stat stat) throws KeeperException {
        String[]        nodes;
        ZooKeeperExtended   _zk;
        
        _zk = mc.getZooKeeper();
        nodes = _zk.getString(path, null, stat).split("\n");
        return ImmutableSet.copyOf(nodes);
    }
    
    public ExclusionSet readLatestFromZK(MetaToolOptions options) throws KeeperException {
        String          vBase;
        long            version;
        Stat			stat;
        ZooKeeperExtended   _zk;
        
        _zk = mc.getZooKeeper();
        version = _zk.getLatestVersion(exclusionsPath);
        if (version >= 0) {
	        vBase = getVBase(version);
	        stat = new Stat();
	        return new ExclusionSet(readNodesAsSet(vBase, stat), version, stat.getMzxid());
        } else {
        	return ExclusionSet.emptyExclusionSet(0);
        }
    }
    
    @Override
    public void writeToFile(File file, ExclusionSet exclusionList) throws IOException {
        IOUtil.writeAsLines(file, exclusionList.getServers());
    }

    public void writeToZK(ExclusionSet exclusionSet) throws IOException, KeeperException {
        writeToZK(exclusionSet, null);
    }
    
    @Override
    public String writeToZK(ExclusionSet exclusionSet, MetaToolOptions options) throws IOException, KeeperException {
        String  vBase;
        String  zkVal;
        ZooKeeperExtended   _zk;
        
        _zk = mc.getZooKeeper();
        zkVal = CollectionUtil.toString(exclusionSet.getServers(), "", "", delimiterChar, "");
        vBase = _zk.createString(base +"/" , zkVal, CreateMode.PERSISTENT_SEQUENTIAL);
        /*
        vBase = zk.createString(base +"/" , "", CreateMode.PERSISTENT_SEQUENTIAL);
        for (String entity : exclusionList.getServers()) {
            //System.out.println(vBase +"/"+ entity);
            zk.createString(vBase +"/"+ entity, entity);
        }
        */
        return null;
    }
    
    public long getVersionMzxid(long version) throws KeeperException {
    	Stat	stat;
        ZooKeeperExtended   _zk;
        
        _zk = mc.getZooKeeper();
    	stat = new Stat();
        _zk.getString(getVBase(version), null, stat);    	
    	return stat.getMzxid();
    }
    
    public Map<String,Long> getStartOfCurrentExclusion(Set<String> servers) throws KeeperException {
    	Map<String,Long>	esStarts;
        long    latestExclusionSetVersion;
        Map<String,Set<String>> exclusionSets;
        ZooKeeperExtended   _zk;
        
        _zk = mc.getZooKeeper();
        esStarts = new HashMap<>();
        exclusionSets = new HashMap<>();
        latestExclusionSetVersion = _zk.getLatestVersion(exclusionsPath);
        for (String server : servers) {
        	esStarts.put(server, getStartOfCurrentExclusion(server, latestExclusionSetVersion, exclusionSets));
        }
        return esStarts;
    }
    
    private long getStartOfCurrentExclusion(String server, long latestExclusionSetVersion, Map<String,Set<String>> exclusionSets) throws KeeperException {
    	long	earliestServerVersion;	
        Stat	stat;
    	long	version;

        earliestServerVersion = -1;
        version = latestExclusionSetVersion;
        while (version >= 0) {
        	String		vBase;
        	Set<String>	nodes;
        	
        	vBase = getVBase(version);
            stat = new Stat();
            nodes = exclusionSets.get(vBase);
            if (nodes == null) {
            	nodes = readNodesAsSet(vBase, stat);
            	exclusionSets.put(vBase, nodes);
            }
            if (!nodes.contains(server)) {
            	break;
            }
            earliestServerVersion = version;
        	--version;
        }
        return earliestServerVersion;
    }
}

package com.ms.silverking.cloud.toporing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.LogStreamConfig;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.meta.VersionListener;
import com.ms.silverking.cloud.meta.VersionWatcher;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.cloud.toporing.meta.MetaPaths;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfigurationUtil;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

public class MasterModeDependencyWatcher implements VersionListener {
    private final MetaClient    mc;
    private final MetaPaths     mp;
    private final com.ms.silverking.cloud.dht.meta.MetaClient	dhtMC;
    private final RingTree		masterRingTree;
    private final Topology		topology;
    private final long			ringConfigVersion;
    private final NamedRingConfiguration  ringConfig;
    private ResolvedReplicaMap	existingReplicaMap;
    
    public static final boolean	verbose = true;
    
    private static final String	logFileName = "MasterModeDependencyWatcher.out";
    
    public MasterModeDependencyWatcher(SKGridConfiguration gridConfig, MasterModeDependencyWatcherOptions options) throws IOException, KeeperException {
        ZooKeeperConfig	zkConfig;
        long			intervalMillis;
        long    		topologyVersion;
        long			configInstanceVersion;
        RingTree		existingTree;
        Pair<RingTree,Triple<String,Long,Long>>	masterRingTreeReadPair;
        Triple<String,Long,Long>	masterRingAndVersionPair;
        
        LogStreamConfig.configureLogStreams(gridConfig, logFileName);        
        
        dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gridConfig);
        zkConfig = dhtMC.getZooKeeper().getZKConfig();
        ringConfig = NamedRingConfigurationUtil.fromGridConfiguration(gridConfig);
        mc = new MetaClient(ringConfig, zkConfig);
        mp = mc.getMetaPaths();
        
        masterRingTreeReadPair = readMasterRingTree(dhtMC, dhtMC.getDHTConfiguration());
        masterRingTree = masterRingTreeReadPair.getV1();
        masterRingAndVersionPair = masterRingTreeReadPair.getV2();
        ringConfigVersion = masterRingAndVersionPair.getV2();
        
        topologyVersion = dhtMC.getZooKeeper().getLatestVersion(mp.getTopologyPath());
        topology = new TopologyZK(mc.createCloudMC()).readFromZK(topologyVersion, null);
        
        configInstanceVersion = dhtMC.getZooKeeper().getLatestVersion(mp.getConfigInstancePath(ringConfigVersion));
        existingTree = SingleRingZK.readTree(mc, ringConfigVersion, configInstanceVersion);
		existingReplicaMap = existingTree.getResolvedMap(ringConfig.getRingConfiguration().getRingParentName(), new ReplicaNaiveIPPrioritizer());
        
        intervalMillis = options.watchIntervalSeconds * 1000;
        new VersionWatcher(mc, mp.getExclusionsPath(), this, intervalMillis);
        new VersionWatcher(dhtMC, dhtMC.getMetaPaths().getInstanceExclusionsPath(), this, intervalMillis);
    }
    
    private RingTree readLatestRingTree(MetaClient mc) throws KeeperException, IOException {
        RingTree    ringTree;
        long        ringConfigVersion;
        long        configInstanceVersion;
        ZooKeeperExtended	zk;

        zk = mc.getZooKeeper();
        ringConfigVersion = zk.getLatestVersion(mp.getConfigPath());
        configInstanceVersion = mc.getLatestConfigInstanceVersion(ringConfigVersion);
        if (configInstanceVersion < 0) {
        	throw new RuntimeException("Can't find configInstanceVersion");
        } else {
    		ringTree = SingleRingZK.readTree(mc, ringConfigVersion, configInstanceVersion);
    	}
        return ringTree;
    }
    
    private Pair<RingTree,Triple<String,Long,Long>> readMasterRingTree(com.ms.silverking.cloud.dht.meta.MetaClient dhtMC, DHTConfiguration dhtConfig) throws KeeperException, IOException {
    	DHTRingCurTargetZK	dhtRingCurTargetZK;
    	Triple<String,Long,Long>	masterRingAndVersionPair;
    	RingTree	ringTree;
    	
    	dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMC, dhtConfig);
    	masterRingAndVersionPair = dhtRingCurTargetZK.getMasterRingAndVersionPair();
        if (masterRingAndVersionPair == null) {
        	throw new RuntimeException("Can't find master ring");
        } else {
    		ringTree = SingleRingZK.readTree(mc, masterRingAndVersionPair.getV2(), masterRingAndVersionPair.getV3());
    		return new Pair<>(ringTree, masterRingAndVersionPair);
    	}
    }
    
    @Override
    public void newVersion(String basePath, long version) {        
        if (verbose) {
            System.out.println("newVersion "+ basePath +" "+ version);
        }
        handleExclusionChange();
    }
    
    private Map<String,Long> createBuildMap(ZooKeeperExtended zk) throws KeeperException {
    	Map<String,Long>	b;
        long    exclusionVersion;
        long    instanceExclusionVersion;

        exclusionVersion = zk.getLatestVersion(mp.getExclusionsPath());
        instanceExclusionVersion = zk.getLatestVersion(dhtMC.getMetaPaths().getInstanceExclusionsPath());
        
    	b = new HashMap<>();
        b.put(mp.getExclusionsPath(), exclusionVersion);
        b.put(dhtMC.getMetaPaths().getInstanceExclusionsPath(), instanceExclusionVersion);
        return b;
	}
    
    private void handleExclusionChange() {
    	synchronized (this) {
	    	try {
		        Map<String,Long> curBuild;
		        ExclusionSet exclusionSet;
		        ExclusionSet instanceExclusionSet;
		        long    exclusionVersion;
		        long    instanceExclusionVersion;
		        ZooKeeperExtended   zk;
		        ExclusionSet	mergedExclusionSet;
		        RingTree	existingRingTree;
		        RingTree	newRingTree;
		        String	newInstancePath;
		        ResolvedReplicaMap	newReplicaMap;
		        
		        zk = mc.getZooKeeper();
		        curBuild = createBuildMap(zk);
		        exclusionVersion = curBuild.get(mp.getExclusionsPath());
		        instanceExclusionVersion = curBuild.get(dhtMC.getMetaPaths().getInstanceExclusionsPath());
		        
		        exclusionSet = new ExclusionSet(new ServerSetExtensionZK(mc, mc.getMetaPaths().getExclusionsPath()).readFromZK(exclusionVersion, null));
		        try {
		        	instanceExclusionSet = new ExclusionSet(new ServerSetExtensionZK(mc, dhtMC.getMetaPaths().getInstanceExclusionsPath()).readFromZK(instanceExclusionVersion, null));
		        } catch (Exception e) {
		        	Log.warning("No instance ExclusionSet found");
		        	instanceExclusionSet = ExclusionSet.emptyExclusionSet(0);
		        }
		        mergedExclusionSet = ExclusionSet.union(exclusionSet, instanceExclusionSet);
		        newRingTree = RingTreeBuilder.removeExcludedNodes(masterRingTree, mergedExclusionSet);
		        
		        newReplicaMap = newRingTree.getResolvedMap(ringConfig.getRingConfiguration().getRingParentName(), new ReplicaNaiveIPPrioritizer());
		        if (!existingReplicaMap.equals(newReplicaMap)) {
		            newInstancePath = mc.createConfigInstancePath(ringConfigVersion); 
			        SingleRingZK.writeTree(mc, topology, newInstancePath, newRingTree);
			        Log.warningf("RingTree written to ZK: %s", newInstancePath);
		        	existingReplicaMap = newReplicaMap;
	    		} else {
	    			Log.warning("RingTree unchanged. No ZK update.");
	    		}
	    	} catch (Exception e) {
	    		e.printStackTrace();
	    		Log.logErrorWarning(e, "handleExclusionChange() failed");
	    	}
    	}
    }

	public static void main(String[] args) {
		try {
			MasterModeDependencyWatcherOptions	options;
            CmdLineParser       parser;			
			
            options = new MasterModeDependencyWatcherOptions();
            parser = new CmdLineParser(options);
            try {
    			MasterModeDependencyWatcher	dw;
                SKGridConfiguration	gc;

                parser.parseArgument(args);
                gc = SKGridConfiguration.parseFile(options.gridConfig);
                TopoRingConstants.setVerbose(true);
                gc = SKGridConfiguration.parseFile(options.gridConfig);
                dw = new MasterModeDependencyWatcher(gc, options);
                ThreadUtil.sleepForever();
            } catch (CmdLineException cle) {
                System.err.println(cle.getMessage());
                parser.printUsage(System.err);
	            System.exit(-1);
            }
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}

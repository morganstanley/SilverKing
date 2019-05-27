package com.ms.silverking.cloud.toporing;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.management.MetaToolModuleBase;
import com.ms.silverking.cloud.management.MetaToolOptions;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.topology.Topology;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.toporing.meta.MetaClient;
import com.ms.silverking.cloud.toporing.meta.MetaPaths;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

// FUTURE - think about this class

// MAKE THIS CLASS USE A SINGLE ZNODE 

// Note that it is possible to always compute maps when needed rather than store.
// We store maps rather than always compute since:
//  - it provides an easy-to-work-with, authoritative record of map versions over time
//  - it decouples the computation code from DHTNodes

public class SingleRingZK extends MetaToolModuleBase<TopologyRing,MetaPaths> {
    private final Topology  topology;
    private final NodeClass nodeClass;
    //private final String    storagePolicyName;
    private WeightSpecifications  weightSpecs;
    private String configInstancePath;
    
    private static final String validString = "valid";
    private static final String versionNode = "version";
    private static final String storagePolicyNameNode = "storagePolicyName";
    
    private static final int   validCheckIntervalMillis = 2 * 1000;
    private static final int   mapNamesRetryIntervalSeconds = 10;
    
    //private static final String mapNameDelimiter = "_";
    
    public SingleRingZK(MetaClient mc, NodeClass nodeClass, Topology topology, 
                        String configInstancePath, String parentID) 
                        throws KeeperException, IOException {
        super(mc, getBase(configInstancePath, parentID));
        this.nodeClass = nodeClass;
        this.configInstancePath = configInstancePath;
        this.topology = topology;
    }

    public SingleRingZK(MetaClient mc, NodeClass nodeClass, long topologyVersion, 
            String configInstancePath, String parentID) 
            throws KeeperException, IOException {
    	this(mc, nodeClass, topologyVersion != VersionedDefinition.NO_VERSION ? new TopologyZK(mc.createCloudMC()).readFromZK(topologyVersion, null) : null, 
    			configInstancePath, parentID);
    }
    
    // For use in static ring creation
    public SingleRingZK(MetaClient mc, Topology topology,
			String configInstancePath, String parentID) throws KeeperException, IOException {
		this(mc, null, topology, configInstancePath, parentID);
	}
    
    /*
    public SingleRingZK(MetaClient mc, NodeClass nodeClass, long topologyVersion, 
            String configInstancePath, String parentID, String storagePolicyName) 
            throws KeeperException, IOException {
        super(mc, getBase(configInstancePath, parentID, storagePolicyName));
        this.nodeClass = nodeClass;
        this.storagePolicyName = storagePolicyName;
        topology = new TopologyZK(mc.createCloudMC()).readFromZK(topologyVersion);
    }
    
    public SingleRingZK(MetaClient mc, NodeClass nodeClass, long topologyVersion, 
            String configInstancePath, String mapName) throws KeeperException, IOException {
        this(mc, nodeClass, topologyVersion, configInstancePath, mapName.split(mapNameDelimiter)[0], mapName.split(mapNameDelimiter)[1]);
    }
    */
    
	private void writeEntry(RingEntry ringEntry, String path) throws KeeperException {
        RingRegion  region;
        
        region = ringEntry.getRegion();
        zk.createString(path +"/"+ region.toZKString(), ringEntry.getPrimaryOwnersZKString() +"::"+ ringEntry.getSecondaryOwnersZKString());
    }    
    
    @Override
    public TopologyRing readFromFile(File file, long version) throws IOException {
        throw new RuntimeException("not currently supported");
    }
    
    // FUTURE - Deprecate?
    // in general the write/read/unresolved code differences are a bit unclean
    //public void setWeightSpecs(WeightSpecifications weightSpecs) {
    //    this.weightSpecs = weightSpecs;
    //}
    
    private void waitUntilValid() throws KeeperException {
        while (!isValidConfigInstancePath()) {
            ThreadUtil.sleep(validCheckIntervalMillis);
        }
        while (!isValidRing()) {
            ThreadUtil.sleep(validCheckIntervalMillis);
        }
    }
    
    private boolean isValidRing() throws KeeperException {
        return zk.getString(base).equals(validString);
    }
    
    private boolean isValidConfigInstancePath() throws KeeperException {
        return zk.getString(configInstancePath).equals(validString);
    }
    
    //private static boolean isValidRing(MetaClient mc, String configInstancePath, String parentID, String policyName, long version) throws KeeperException {
    //    ZooKeeperExtended   zk;
    //    
    //    zk = mc.getZooKeeper();
    //    return zk.getString(getBase(configInstancePath, parentID, policyName)).equals(validString);
    //}
    private static boolean isValidRing(MetaClient mc, String configInstancePath, String parentID, long version) throws KeeperException {
        ZooKeeperExtended   zk;
        
        zk = mc.getZooKeeper();
        return zk.getString(getBase(configInstancePath, parentID)).equals(validString);
    }
    
    //private static String getBase(String configInstancePath, String parentID, String policyName) {
    //    return configInstancePath +"/"+ parentID +"_"+ policyName;
    //}
    private static String getBase(String configInstancePath, String parentID) {
        return configInstancePath +"/"+ parentID;
    }
    
    /*
    private static String topologyNodeNameAndStoragePolicyNameToMapName(String topologyNodeName, String policyName) {
        return topologyNodeName + mapNameDelimiter + policyName;
    }

    private static Pair<String,String> mapNameToTopologyNodeNameAndStoragePolicyName(String mapName) {
        String[]    names;
        
        names = mapName.split(mapNameDelimiter);
        return new Pair<>(names[0], names[1]);
    }
    */
    
    @Override
    public TopologyRing readFromZK(long version, MetaToolOptions options) throws KeeperException {
        List<String>    nodes;
        SingleRing      singleRing;
        String          storagePolicyName;
        Map<String,String>	defs;
        Set<String>	paths;

        nodes = zk.getChildren(base);
        storagePolicyName = nodes.get(nodes.indexOf(storagePolicyNameNode));
        nodes.remove(versionNode);
        nodes.remove(storagePolicyNameNode);
        singleRing = new SingleRing(nodeClass, version, storagePolicyName);
        paths = new HashSet<>();
        for (String node : nodes) {
            paths.add(base +"/"+ node);
        }
        defs = zk.getStrings(paths);
        for (String node : nodes) {
            String  def;
            RingEntry   entry;
            
            def = defs.get(base +"/"+ node);
            if (TopoRingConstants.verbose) {
                System.out.println("base: "+ base);
                System.out.println("def: "+ def);
            }
            entry = RingEntry.parseZKDefs(topology, node, def);
            singleRing.addEntry(entry);
        }
        // pass empty weight specifications since only the reallocator
        // needs them
        // FIXME - think about whether re-allocator will use this code
        singleRing.freeze(new WeightSpecifications(0));
        return singleRing;
    }
    
    /*
    @Override
    public TopologyRing readFromZK(long version, MetaToolOptions options) throws KeeperException {
        List<String>    nodes;
        SingleRing      singleRing;
        String          storagePolicyName;

        nodes = zk.getChildren(base);
        storagePolicyName = nodes.get(nodes.indexOf(storagePolicyNameNode));
        nodes.remove(versionNode);
        nodes.remove(storagePolicyNameNode);
        singleRing = new SingleRing(nodeClass, version, storagePolicyName);
        for (String node : nodes) {
            String  def;
            RingEntry   entry;
            
            def = zk.getString(base +"/"+ node);
            if (TopoRingConstants.verbose) {
                System.out.println("base: "+ base);
                System.out.println("def: "+ def);
            }
            entry = RingEntry.parseZKDefs(topology, node, def);
            singleRing.addEntry(entry);
        }
        // pass empty weight specifications since only the reallocator
        // needs them
        // FIXME - think about whether re-allocator will use this code
        singleRing.freeze(new WeightSpecifications(0));
        return singleRing;
    }
    */
    
    @Override
    public void writeToFile(File file, TopologyRing instance) throws IOException {
        throw new RuntimeException("not currently supported");
    }

    @Override
    public String writeToZK(TopologyRing ring, MetaToolOptions options) throws IOException, KeeperException {
        zk.createAllParentNodes(base);
        zk.create(base);
        zk.createString(base +"/"+ storagePolicyNameNode, ring.getStoragePolicyName());
        zk.createInt(base +"/"+ versionNode, (int)topology.getVersion());
        for (RingEntry member : ring.getMembers()) {
            assert base != null;
            writeEntry(member, base);
        }
        zk.setString(base, validString);
        return null;
    }
    
    private void setConfigInstanceValid() throws KeeperException {
        zk.setString(configInstancePath, validString);
    }

    // For static ring creation
    public static void writeTree(MetaClient mc, Topology topology, String configInstancePath, 
            RingTree ringTree) throws KeeperException, IOException {
        SingleRingZK    singleRingZK;
        
        singleRingZK = null;
        for (Map.Entry<String, TopologyRing> entry : ringTree.getMaps().entrySet()) {
            //String          parentID;
            //String          storagePolicyName;
            
            //parentID = entry.getKey().getV1();
            //storagePolicyName = entry.getKey().getV2();
            //singleRingZK = new SingleRingZK(mc, null, topologyVersion, configInstancePath, parentID, storagePolicyName);
            singleRingZK = new SingleRingZK(mc, topology, configInstancePath, entry.getKey());
            singleRingZK.writeToZK(entry.getValue(), null);
        }
        if (singleRingZK != null) {
            singleRingZK.setConfigInstanceValid();
        }
    }
    
    public static void writeTree(MetaClient mc, long topologyVersion, String configInstancePath, 
            RingTree ringTree) throws KeeperException, IOException {
        SingleRingZK    singleRingZK;
        
        singleRingZK = null;
        for (Map.Entry<String, TopologyRing> entry : ringTree.getMaps().entrySet()) {
            //String          parentID;
            //String          storagePolicyName;
            
            //parentID = entry.getKey().getV1();
            //storagePolicyName = entry.getKey().getV2();
            //singleRingZK = new SingleRingZK(mc, null, topologyVersion, configInstancePath, parentID, storagePolicyName);
            singleRingZK = new SingleRingZK(mc, null, topologyVersion, configInstancePath, entry.getKey());
            singleRingZK.writeToZK(entry.getValue(), null);
        }
        if (singleRingZK != null) {
            singleRingZK.setConfigInstanceValid();
        }
    }

    public static void writeTree(MetaClient mc, long topologyVersion, String configInstancePath, 
                                 RingTree ringTree, Node parent/*, String storagePolicyName*/) 
                                         throws KeeperException, IOException {
        assert configInstancePath != null;
        if (parent.hasChildren()) {
            SingleRingZK    singleRingZK;
            
            //singleRingZK = new SingleRingZK(mc, parent.getNodeClass(), topologyVersion, configInstancePath, parent.getIDString(), storagePolicyName);
            singleRingZK = new SingleRingZK(mc, parent.getNodeClass(), topologyVersion, configInstancePath, parent.getIDString());
            singleRingZK.writeToZK(ringTree.getMap(parent.getIDString()), null);
            for (Node child : parent.getChildren()) {
                writeTree(mc, topologyVersion, configInstancePath, ringTree, child); 
            }
        }
    }
    
    public static void waitUntilValid(MetaClient mc, String configInstancePath, long ringVersion) throws KeeperException, IOException {
        String                      ringInstancePath;
        long                        topologyVersion;
        SingleRingZK                singleRingZK;
        
        assert configInstancePath != null;        
        ringInstancePath = mc.getMetaPaths().getRingInstancePath(configInstancePath, ringVersion);
        topologyVersion = VersionedDefinition.NO_VERSION;
        singleRingZK = new SingleRingZK(mc, null/*parent.getNodeClass()*/, topologyVersion, ringInstancePath, "");
        
        while (!singleRingZK.isValidConfigInstancePath()) {
            ThreadUtil.sleep(validCheckIntervalMillis);
        }
    }

    public static InstantiatedRingTree readTree(MetaClient mc, Pair<Long,Long> ringVersion)
    		throws KeeperException, IOException {
    	return readTree(mc, ringVersion.getV1(), ringVersion.getV2());
    }
    
    public static boolean treeIsValid(MetaClient mc, Pair<Long,Long> ringVersion) throws KeeperException {
    	return treeIsValid(mc, ringVersion.getV1(), ringVersion.getV2());
    }
    
    public static boolean treeIsValid(MetaClient mc, long ringConfigVersion, long configInstanceVersion) throws KeeperException {
        String	ringInstancePath;
        
        ringInstancePath = mc.getMetaPaths().getRingInstancePath(ringConfigVersion, configInstanceVersion);
        return mc.getZooKeeper().getString(ringInstancePath).equals(validString);
    }
    
    public static InstantiatedRingTree readTree(MetaClient mc, long ringConfigVersion, long configInstanceVersion) 
            throws KeeperException, IOException {
        ZooKeeperExtended           zk;
        List<String>                mapNames;
        //Map<Pair<String,String>,TopologyRing>    maps;
        Map<String,TopologyRing>    maps;
        Topology                    topology;
        String                      ringInstancePath;
        //WeightSpecifications        weightSpecs;
        com.ms.silverking.cloud.meta.MetaClient cloudMC;
        long                        topologyVersion;
        long                        ringCreationTime;

        zk = mc.getZooKeeper();
        cloudMC = mc.createCloudMC();
        ringInstancePath = mc.getMetaPaths().getRingInstancePath(ringConfigVersion, configInstanceVersion);
        ringCreationTime = zk.getStat(ringInstancePath).getCtime();
        mapNames = zk.getChildren(ringInstancePath);
        //weightSpecs = new WeightsZK(mc).readFromZK(weightsVersion);
        while (mapNames.size() == 0) {
            Log.warning(String.format("No maps for: %s; waiting.", ringInstancePath));
            ThreadUtil.sleepSeconds(mapNamesRetryIntervalSeconds); // FUTURE - move this retry loop out to common, primarily non-polling function
            mapNames = zk.getChildren(ringInstancePath);
        }
        if (DHTConstants.isDaemon || Log.levelMet(Level.INFO)) {
        	Log.warning(String.format("mapNames found: %d", mapNames.size()));
        }
        topologyVersion = VersionedDefinition.NO_VERSION;
        for (String mapName : mapNames) {
        	String	tp;
            long    tv;
            
            tp = ringInstancePath +"/"+ mapName +"/"+ versionNode;
            tv = zk.getInt(tp);
            Log.warningf("topology %s %d", tp, tv);
            if (topologyVersion == VersionedDefinition.NO_VERSION) {
                topologyVersion = tv;
            } else {
                if (tv != topologyVersion) {
                    throw new RuntimeException("inconsistent topologyVersions: "+ ringInstancePath);
                }
            }
        }
        topology = new TopologyZK(cloudMC).readFromZK(topologyVersion, null);
        
        maps = new HashMap<>();
        for (String mapName : mapNames) {            
            SingleRingZK    singleRingZK;
            TopologyRing    topologyRing;

            singleRingZK = new SingleRingZK(mc, null/*parent.getNodeClass()*/, topologyVersion, ringInstancePath, mapName);
            if (TopoRingConstants.verbose) {
                Log.warning("Waiting for valid ring: ", ringInstancePath +" "+ mapName);
            }
            singleRingZK.waitUntilValid();
            if (TopoRingConstants.verbose) {
                Log.warning("Valid: ", ringInstancePath +" "+ mapName);
            }
            //singleRingZK.setWeightSpecs(weightSpecs);
            // FIXME - version in single ring is unused
            // verify that we can remove version in SingleRing
            topologyRing = singleRingZK.readFromZK(0L, null);
            //maps.put(mapNameToTopologyNodeNameAndStoragePolicyName(mapName), topologyRing);
            maps.put(mapName, topologyRing);
        }
        return new InstantiatedRingTree(topology, maps, new Pair<>(ringConfigVersion, configInstanceVersion), ringCreationTime);
    }
}
 
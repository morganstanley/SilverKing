package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.meta.MetaClientBase;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;

public class MetaClient extends MetaClientBase<MetaPaths> {
    private final String    dhtName;
    
    public MetaClient(String dhtName, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
        this(new NamedDHTConfiguration(dhtName, null), zkConfig);
    }
    
    public MetaClient(NamedDHTConfiguration dhtConfig, ZooKeeperConfig zkConfig, Watcher watcher) throws IOException, KeeperException {
        super(new MetaPaths(dhtConfig), zkConfig, watcher);
        this.dhtName = dhtConfig.getDHTName();
    }

    public MetaClient(NamedDHTConfiguration dhtConfig, ZooKeeperConfig zkConfig) throws IOException, KeeperException {
        this(dhtConfig, zkConfig, null);
    }
    
    public MetaClient(ClientDHTConfiguration clientDHTConfig) throws IOException, KeeperException {
    	this(clientDHTConfig.getName(), clientDHTConfig.getZKConfig());
    }

    public MetaClient(SKGridConfiguration skGridConfig) throws IOException, KeeperException {
    	this(skGridConfig.getClientDHTConfiguration());
    }
    
    public String getDHTName() {
        return dhtName;
    }
    
    public DHTConfiguration getDHTConfiguration() throws KeeperException {
        String  def;
        long    version;
        String  latestPath;
        long    zkid;
        ZooKeeperExtended   zk;
        
        zk = getZooKeeper();
        latestPath = zk.getLatestVersionPath(getMetaPaths().getInstanceConfigPath());
        version = zk.getLatestVersionFromPath(latestPath);
        def = zk.getString(latestPath);
        zkid = zk.getStat(latestPath).getMzxid();
        //System.out.printf("\tzkid %x\n", zkid);
        return DHTConfiguration.parse(def, version, zkid);
    }
    
    /*
    private static final int    maxOpAttempts = 4;
    private static final int    opRetryInterval = 4 * 1000;
    
    private static final int    pollIntervalMin = 5 * 1000;
    private static final int    pollIntervalMax = 15 * 1000;
    private static final int    waitInterval = 100 * 1000;
    
    public static final byte[]  NULL_ARRAY = new byte[0];
    public static final byte[]  ZERO_ARRAY = NumConversion.intToBytes(0);
        
    public void genesis() throws KeeperException {
        zk.create(paths.dhtGlobalBase, paths.dhtGlobalBase.getBytes());
    }   
    
    public void createBootID() throws KeeperException {
        String bootId = Long.toString(System.currentTimeMillis());
        zk.setString(paths.bootIdPath, bootId);
    }

    public void start() throws KeeperException {
        zk.create(paths.startTokenPath, NULL_ARRAY);
    }
    
    public void stop() throws KeeperException {
        zk.delete(paths.startTokenPath);
    }
    
    public void waitForStart() throws KeeperException {
        System.out.println("Waiting for start "+ paths.startTokenPath +"...");
        try {
            while (zk.exists(paths.startTokenPath, true) == null) {
                synchronized (this) {
                    this.wait(waitInterval);
                }
                //ThreadUtil.randomSleep(pollIntervalMin, pollIntervalMax);
                System.out.println("...");
            }
        } catch (InterruptedException ie) {
            throw new RuntimeException("panic");
        }
        System.out.println("Started.");
    }

    public String getBootID() throws KeeperException {
        return zk.getString(paths.bootIdPath);
    }

    public void setNodeLocalBasePath(String nodeLocalBasePath) throws KeeperException {
        zk.setString(paths.nodeLocalBasePathPath, nodeLocalBasePath);
    }
    
    public String getNodeLocalBasePath() throws KeeperException {
        return zk.getString(paths.nodeLocalBasePathPath);
    }
    
    public String getPathString(String path) throws KeeperException {
        return zk.getString(path);
    }
    
    public int getNumNodes() throws KeeperException {
        return zk.getChildren(paths.nodesBase).size();
    }
    
    public List<RemoteRef> getNodes() throws KeeperException {
        return _getNodes(paths.nodesBase);
    }
    
    public List<RemoteRef> getZnodes() throws KeeperException {
        return _getNodes(paths.znodesBase);
    }
    
    public List<RemoteRef> getBlacklistedNodes() throws KeeperException {
        return _getNodes(paths.blacklistedNodesBase);
    }

    private List<RemoteRef> _getNodes(String base) throws KeeperException {
        List<String>    nodes;
        List<RemoteRef> nodeRefs;
        
        nodes = zk.getChildren(base);
        nodeRefs = new ArrayList<RemoteRef>(nodes.size());
        for (String node : nodes) {
            nodeRefs.add(new RemoteRef(node));
        }
        return nodeRefs;
    }   

    private String getNodesPath(RemoteRef nodeRef) {
        return paths.nodesBase +"/"+ nodeRef.toString();
    }
    
    public boolean nodeExists(RemoteRef nodeRef) throws KeeperException {
        return zk.exists(getNodesPath(nodeRef));
    }

    public Set<RemoteRef> getNodesSet() throws KeeperException {
        Set<RemoteRef>  nodeRefs;
        
        nodeRefs = new HashSet<RemoteRef>();
        addNodesToCollection(nodeRefs, paths.nodesBase);
        return nodeRefs;
    }
    
    public List<RemoteRef> getBootNodes() throws KeeperException {
        ArrayList<RemoteRef>    nodeRefs;
        
        nodeRefs = new ArrayList<RemoteRef>();
        addNodesToCollection(nodeRefs, paths.bootNodesBase);
        return nodeRefs;
    }
    
    public List<RemoteRef> getClassNodesForClassMember(RemoteRef node) throws KeeperException {
        for (Map.Entry<String, List<RemoteRef>> entry : getClassMap().entrySet()) {
            if (entry.getValue().contains(node)) {
                return entry.getValue();
            }
        }
        return new ArrayList<RemoteRef>(0);
    }
    
    public String getNodeClass(Map<String, List<RemoteRef>> classMap, RemoteRef node) {
        for (Map.Entry<String, List<RemoteRef>> entry : classMap.entrySet()) {
            if (entry.getValue().contains(node)) {
                return entry.getKey();
            }
        }
        return null;
    }
    
    public Map<String, List<RemoteRef>> getClassMap() throws KeeperException {
        Map<String, List<RemoteRef>>    classMap;
        List<String>    classes;
        
        classMap = new HashMap<String, List<RemoteRef>>();
        classes = getClassList();
        for (String className : classes) {
            classMap.put(className, getClassNodesList(className));
        }
        return classMap;
    }
    
    public List<String> getClassList() throws KeeperException {
        return zk.getChildren(paths.classBase);
    }
    
    public List<RemoteRef> getClassNodesList(String className) throws KeeperException {
        List<RemoteRef> nodeRefs;
        
        nodeRefs = new ArrayList<RemoteRef>();
        addNodesToCollection(nodeRefs, paths.classBase +"/"+ className);
        return nodeRefs;
    }
    
    private void addNodesToCollection(Collection<RemoteRef> collection, String path) throws KeeperException {
        for (String node : zk.getChildren(path)) {
            collection.add(new RemoteRef(node));
        }
    }
    */
}

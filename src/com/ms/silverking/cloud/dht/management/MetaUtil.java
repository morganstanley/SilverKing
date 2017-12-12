package com.ms.silverking.cloud.dht.management;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.MetaPaths;
import com.ms.silverking.cloud.dht.meta.NamedDHTConfiguration;
import com.ms.silverking.cloud.dht.meta.NamespaceLinksZK;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.HostGroupTableZK;
import com.ms.silverking.cloud.meta.PassiveNodeSet;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.topology.TopologyZK;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.io.IOUtil;

/*
 * Given a DHT name, this utility can provide:
 *  - the list of host groups used by that DHT
 *  - the list of servers in a host group for that DHT
 *  - the list of excluded servers for that DHT
 */
public class MetaUtil {
    private final MetaClient  mc;
    private final MetaPaths   mp;
    private final long        dhtConfigVersion;
    private final long        dhtConfZkId;
    private final String      dhtRingName;
    private final DHTConfiguration    dhtConfig;
    private final DHTConfigurationZK  dhtConfZk;
    private final ZooKeeperExtended   zk;
    private final ExclusionSet exclusions;
    private final PassiveNodeSet passiveNodes;
    private final PrintStream  out;
    
    //readRingFromZk
    private final com.ms.silverking.cloud.toporing.meta.MetaClient    ringMC;
    private final NamedRingConfiguration namedRingConfig;
    private final RingConfiguration   ringConfig;
    private final long                ringConfigVersion;
    private final long                configInstanceVersion;

    private final com.ms.silverking.cloud.meta.MetaClient             cloudMC;
    private final TopologyZK          topoConfZk;
    private final com.ms.silverking.cloud.topology.Topology topo;
    
    private static final boolean    debug = true;
    
    public MetaUtil(String dhtName, ZooKeeperConfig zkConfig, long dhtVersion, File target) throws KeeperException,  IOException {
        String  dhtVersionPath;
        long    cloudConfigVersion;
        long    exclusionsVersion;
        long    passiveNodesVersion;
        MetaClient  _mc;
        NamedDHTConfiguration   namedDHTConfig;
        
        _mc = new MetaClient(dhtName, zkConfig);
        zk = _mc.getZooKeeper();
        
        mp = _mc.getMetaPaths();
        
        if (debug) {
            System.out.println("getting latest version: "+ mp.getInstanceConfigPath());
        }
        
        if (dhtVersion == MetaUtilOptions.dhtVersionUnspecified) {
            dhtConfigVersion = _mc.getZooKeeper().getLatestVersion(mp.getInstanceConfigPath());
        } else {
            dhtConfigVersion = dhtVersion;
        }
        
        if (debug) {
            System.out.println("MetaUtil dhtConfigVersion: " + dhtConfigVersion);
        }
        
        dhtConfZk = new DHTConfigurationZK(_mc);
        dhtConfig = dhtConfZk.readFromZK(dhtConfigVersion, null);
        namedDHTConfig = new NamedDHTConfiguration(dhtName, dhtConfig);
        
        mc = new MetaClient(namedDHTConfig, zkConfig);
        
        if (debug) {
            System.out.println("dhtConfig: "+ dhtConfig);
        }
        
        dhtRingName = dhtConfig.getRingName();
        dhtVersionPath = ZooKeeperExtended.padVersionPath(mc.getMetaPaths().getInstanceConfigPath(), dhtConfigVersion);
        dhtConfZkId = zk.getStat(dhtVersionPath).getMzxid();
        
        if (debug) {
            System.out.println("dhtVersionPath: "+ dhtVersionPath + " realDhtConfZkId: " + dhtConfZkId );
        }
        
        NamedRingConfiguration  _namedRingConfig;
        com.ms.silverking.cloud.toporing.meta.MetaClient    _ringMC;
        _namedRingConfig = new NamedRingConfiguration(dhtRingName, RingConfiguration.emptyTemplate);        
        _ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(_namedRingConfig, zkConfig);
        ringConfigVersion = getVersionPriorTo_floored(_ringMC.getMetaPaths().getConfigPath(), dhtConfZkId);
        ringConfig = new RingConfigurationZK(_ringMC).readFromZK(ringConfigVersion, null);

        namedRingConfig = new NamedRingConfiguration(dhtRingName, ringConfig);        
        ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(namedRingConfig, zkConfig);

        //exclusionsVersion = getVersionPriorTo_floored(ringMC.getMetaPaths().getExclusionsPath(), dhtConfZkId);
        exclusionsVersion = getLatestVersion(ringMC.getMetaPaths().getExclusionsPath());
        System.err.printf("%s\t%d\n", ringMC.getMetaPaths().getExclusionsPath(), exclusionsVersion);
        exclusions = new ExclusionSet( 
                new ServerSetExtensionZK(ringMC, ringMC.getMetaPaths().getExclusionsPath()).readFromZK(exclusionsVersion, null) );
        
        if (dhtConfig.hasPassiveNodeHostGroups()) {
	        passiveNodesVersion = getVersionPriorTo_floored(mc.getMetaPaths().getPassiveNodesPath(), dhtConfZkId);
	        passiveNodes = new PassiveNodeSet( 
	                new ServerSetExtensionZK(mc, mc.getMetaPaths().getPassiveNodesPath()).readFromZK(passiveNodesVersion, null) );
        } else {
        	passiveNodes = PassiveNodeSet.emptySet();
        }
        
        configInstanceVersion = getVersionPriorTo_floored(ringMC.getMetaPaths().getConfigInstancePath(ringConfigVersion), dhtConfZkId);
        
        cloudMC = new com.ms.silverking.cloud.meta.MetaClient(ringConfig.getCloudConfiguration(),zkConfig);
        cloudConfigVersion = getVersionPriorTo_floored(cloudMC.getMetaPaths().getTopologyPath(), dhtConfZkId);
        
        if (debug) {
            System.out.println("TopologyPath: "+ cloudMC.getMetaPaths().getTopologyPath() + " version: " + cloudConfigVersion);
            System.out.println("dhtConfZkId: "+ dhtConfZkId );
        }
            
        topoConfZk = new TopologyZK(cloudMC);
        topo = topoConfZk.readFromZK(cloudConfigVersion, null);
        
        if (target == null) {
            out = System.out;
        } else {
            out = new PrintStream(new FileOutputStream(target));
        }
    }
    
    public MetaUtil(String dhtName, String zkString, long dhtVersion, File target) throws KeeperException,  IOException {
    	this(dhtName, new ZooKeeperConfig(zkString), dhtVersion, target);
    }
    
    public MetaUtil(String name, ZooKeeperConfig zkConfig, long dhtVersion) throws KeeperException, IOException {
    	this(name, zkConfig, dhtVersion, null);
    }
    
	public DHTConfiguration getDHTConfiguration() {
    	return dhtConfig;
    }
	
	public MetaClient getDHTMC() {
		return mc;
	}
    
    public RingConfiguration getRingConfiguration() {
    	return ringConfig;
    }
    
    public com.ms.silverking.cloud.toporing.meta.MetaClient getRingMC() {
    	return ringMC;
    }
    
    private long getLatestVersion(String path) throws KeeperException {
        return mc.getZooKeeper().getLatestVersion(path);
        //return mc.getZooKeeper().getLatestVersion(mp.getInstanceConfigPath());
    }
    
    private long getVersionPriorTo_floored(String path, long zkid) throws KeeperException {
        long    version;
        
        version = mc.getZooKeeper().getVersionPriorTo(path, zkid);
        return version == -1 ? 0 : version;
    }
    
    public List<String> getDHTServersFromZk() throws KeeperException, IOException {        
        //System.out.println("ServerList");
        //writeNodeAndChildren(topo.getRoot());        
        List<Node>      serverNodeList;
        List<String>    serverNameList;
        
        serverNodeList = topo.getRoot().getAllDescendants(NodeClass.server);
        serverNameList = new ArrayList<>(serverNodeList.size());
        for (Node serverNode : serverNodeList) {
            serverNameList.add(serverNode.getIDString());
        }
        return serverNameList;
    }
    
    private HostGroupTable readHostGroupTable() throws KeeperException {
        long            version;

        version = getVersionPriorTo_floored(cloudMC.getMetaPaths().getHostGroupPath(), dhtConfZkId);
        return new HostGroupTableZK(cloudMC).readFromZK(version, null);
    }
    
    public void writeNodeAndChildren(com.ms.silverking.cloud.topology.Node node) {
        if(node.hasChildren()) {
            for (Node child : node.getChildren()) {
                writeNodeAndChildren( child );
            }
        } else {
            if(node.getNodeClass() == com.ms.silverking.cloud.topology.NodeClass.server)
                System.out.println( node.getIDString());
        }
    }
    
    private Set<String> getDHTHostGroups() {
        return dhtConfig.getHostGroups();
    }
    
    private void displayDHTHostGroups() {
        for (String hostGroup : getDHTHostGroups()) {
            displayForFiltering(hostGroup);
        }
    }
    
    private void displayDHTExcludedServers() {
        for (String server : exclusions.getServers()) {
            displayForFiltering(server);
        }
    }
    
    private void displayDHTPassiveNodes() {
        for (String server : passiveNodes.getServers()) {
            displayForFiltering(server);
        }
    }
    
    private void displayDHTHostGroupServers(Iterable<String> hostGroups, FilterOption filterOption, 
            String workersFile, String exclusionsFile) throws KeeperException, IOException {
        Set<String>     allowedHostAddresses;
        Set<String>     workers;
        
        workers = new HashSet<>();
        allowedHostAddresses = readHostGroupTable().getHostAddresses(hostGroups);
        for(String dhtServer : getDHTServersFromZk()) {
            if (allowedHostAddresses.contains(dhtServer)) {
                if (filterOption == FilterOption.NoFiltering || !exclusions.contains(dhtServer)) {
                    displayForFiltering(dhtServer);
                    workers.add(dhtServer);
                }
            } else {
                //System.out.println("Ignoring:\t"+ dhtServer);
            }
        }
        
        if (exclusionsFile != null) {
            IOUtil.writeAsLines(new File(exclusionsFile), exclusions.getServers());
        }
        if (workersFile != null) {
            IOUtil.writeAsLines(new File(workersFile), workers);
        }
    }
    
    private void clearLinks() throws IOException, KeeperException {
        NamespaceLinksZK    nlzk;
        
        nlzk = new NamespaceLinksZK(mc);
        nlzk.clearAllZK();
    }

    private Map<String,String> getHostGroupToClassVarsMap() {
        return dhtConfig.getHostGroupToClassVarsMap();
    }
    
    private void displayHostGroupToClassVarsMap() {
    	Map<String,String> hostGroupToClassVars = getHostGroupToClassVarsMap();
        for (String hostGroup : hostGroupToClassVars.keySet()) {
            displayForFiltering(hostGroup + "=" + hostGroupToClassVars.get(hostGroup));
        }
    }

    public void runCommand(MetaUtilOptions options) throws KeeperException, IOException {
        switch (options.command) {
        case GetDHTHostGroups:
            displayDHTHostGroups();
            break;
        case GetDHTHostGroupServers:
            displayDHTHostGroupServers(ImmutableList.copyOf(options.hostGroups.split(",")), 
                    options.filterOption, options.workersFile, options.exclusionsFile);
            break;
        case GetDHTExcludedServers:
            displayDHTExcludedServers();
            break;
        case GetDHTPassiveNodes:
            displayDHTPassiveNodes();
            break;
        case ClearLinks:
            clearLinks();
            break;
        case GetHostGroupToClassVarsMap:
        	displayHostGroupToClassVarsMap();
            break;
        	
        default: throw new RuntimeException("panic");
        }
    }
    
    private void displayForFiltering(String s) {
        out.printf("%s\n", s);
        //out.printf("%s%c%s\n", filterText, filterDelimiter, s);
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            MetaUtil        mu;
            MetaUtilOptions options;
            CmdLineParser   parser;
            
            options = new MetaUtilOptions();
            parser = new CmdLineParser(options);
            try {
                parser.parseArgument(args);
                switch (options.command) {
                case GetDHTHostGroups:
                    break;
                case GetDHTHostGroupServers:
                    if (options.hostGroups == null) {
                        throw new CmdLineException("GetDHTHostGroupServers requires -h hostGroups option");
                    }
                    if (options.filterOption == null) {
                        throw new CmdLineException("GetDHTHostGroupServers requires -f filterOption option");
                    }
                    break;
                case GetDHTExcludedServers:
                    break;
                case GetDHTPassiveNodes:
                    if (options.filterOption == null) {
                        throw new CmdLineException("GetDHTPassiveNodes requires -f filterOption option");
                    }
                    break;
                case ClearLinks:
                    break;
                case GetHostGroupToClassVarsMap:
                    break;
                default: throw new RuntimeException("panic");
                }
            } catch (CmdLineException cle) {
                System.err.println(cle.getMessage());
                parser.printUsage(System.err);
	            System.exit(-1);
            }
            
            mu = new MetaUtil(options.dhtName, options.zkEnsemble, options.dhtVersion, 
                              options.targetFile == null ? null : new File(options.targetFile)) ;
            mu.runCommand(options);
        } catch (Exception e){
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

package com.ms.silverking.cloud.dht.management;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.daemon.DHTNode;
import com.ms.silverking.cloud.dht.daemon.DaemonState;
import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.ClassVars;
import com.ms.silverking.cloud.dht.meta.ClassVarsZK;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.dht.meta.DaemonStateZK;
import com.ms.silverking.cloud.dht.meta.HealthMonitor;
import com.ms.silverking.cloud.dht.meta.IneligibleServerException;
import com.ms.silverking.cloud.dht.meta.InstanceExclusionZK;
import com.ms.silverking.cloud.dht.meta.SuspectsZK;
import com.ms.silverking.cloud.gridconfig.GridConfiguration;
import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.ExclusionZK;
import com.ms.silverking.cloud.meta.HostGroupTableZK;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.cloud.skfs.management.SKFSNamespaceCreator;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.toporing.InstantiatedRingTree;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.SingleRingZK;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.numeric.NumUtil;
import com.ms.silverking.pssh.TwoLevelParallelSSHMaster;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.thread.lwt.LWTThreadUtil;
import com.ms.silverking.util.ArrayUtil;
import com.ms.silverking.util.Arrays;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * <p>Tool responsible for executing most administrative SilverKing commands.
 * E.g. used to stop and start SilverKing instances.</p>
 * 
 * <p>Shell scripts used to launch this and other administrative commands should
 * contain minimal logic. Any "real work" should be done here (and in the other
 * administrative tools' Java implementations.)</p>
 */
public class SKAdmin {
	private final SKGridConfiguration	gc;
	private final SKAdminOptions		options;
	private final com.ms.silverking.cloud.dht.meta.MetaClient	dhtMC;
	private final com.ms.silverking.cloud.meta.MetaClient	cloudMC;
	private final DHTConfiguration	dhtConfig;
	private final ClassVarsZK			classVarsZK;
	private final ClassVars				defaultClassVars;
	private final SuspectsZK			suspectsZK;
	private final NamespaceOptions	skfsNSOptions; 
	private final NamespaceOptions	skfsMutableNSOptions;
	private final NamespaceOptions	skfsFileBlockNSOptions;
	private final NamespaceOptions	skfsDirNSOptions;
	private final String	skGlobalCodebase;
	private final RingConfiguration		ringConfig;
	private final InstantiatedRingTree	ringTree;
	private ExclusionSet			exclusionSet;
	
	private static final String	jreSuffix = "/jre";
	
	/*
	 * In the future, we should probably parallelize some metadata fetches that can be done in parallel in order
	 * to hide the latency of these operations. For now, we do not do this. 
	 */
	//private enum WorkType {GetHostGroups};
	
	//private static final String[]	_skfsNamespaces = {"fb.524288.b", "fb.131072.b", "fb.262144.b", "dht.health"};
	//private static final String[]	_skfsMutableNamespaces = {"dir", "attr"};
	private static final String[]	_skfsNamespaces = {};
	private static final String[]	_skfsMutableNamespaces = {"attr", "dht.health"};
	private static final String[]	_skfsFileBlockNamespaces = {"fb.524288.b", "fb.131072.b", "fb.262144.b"};
	private static final String[]	_skfsDirNamespaces = {"dir"};
	private static final Set<String>	skfsNamespaces = ImmutableSet.copyOf(_skfsNamespaces);
	private static final Set<String>	skfsMutableNamespaces = ImmutableSet.copyOf(_skfsMutableNamespaces);
	private static final Set<String>	skfsFileBlockNamespaces = ImmutableSet.copyOf(_skfsFileBlockNamespaces);
	private static final Set<String>	skfsDirNamespaces = ImmutableSet.copyOf(_skfsDirNamespaces);
	
	private static final int	unsafeWarningCountdown = 10;
	
	private static final String	logFileName = "SKAdmin.out";
	
	public static boolean	exitOnCompletion = true;
	
	public SKAdmin(SKGridConfiguration gc, SKAdminOptions options) throws IOException, KeeperException {
		Pair<RingConfiguration,InstantiatedRingTree>	ringConfigAndTree;
		
		this.gc = gc;
		this.options = options;
		dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gc);
		dhtConfig = dhtMC.getDHTConfiguration();
		suspectsZK = new SuspectsZK(dhtMC);
		
		ringConfigAndTree = getRing(dhtConfig, dhtMC);		
		if (ringConfigAndTree == null) {
			Log.warning("No current ring for this instance");
			ringConfig = null;
			ringTree = null;
			cloudMC = null;
			exclusionSet = null;
			throw new RuntimeException("No current ring for this instance");
		} else {
			ExclusionSet	es1;
			ExclusionSet	es2;
			
			ringConfig = ringConfigAndTree.getV1();
			ringTree = ringConfigAndTree.getV2();
			cloudMC = new com.ms.silverking.cloud.meta.MetaClient(ringConfig.getCloudConfiguration(), dhtMC.getZooKeeper().getZKConfig());
			es1 = new ExclusionZK(cloudMC).readLatestFromZK();	
			if (options.excludeInstanceExclusions) {
				es2 = new InstanceExclusionZK(dhtMC).readLatestFromZK();
			} else {
				es2 = ExclusionSet.emptyExclusionSet(0);
			}
			exclusionSet = ExclusionSet.union(es1, es2);
		}
		
		
		classVarsZK = new ClassVarsZK(dhtMC);
		if (options.defaultClassVars != null) {
			defaultClassVars = DHTConstants.defaultDefaultClassVars.overrideWith(classVarsZK.getClassVars(options.defaultClassVars));
		} else {
			if (dhtConfig.getDefaultClassVars() != null) {
				defaultClassVars = DHTConstants.defaultDefaultClassVars.overrideWith(classVarsZK.getClassVars(dhtConfig.getDefaultClassVars()));
			} else {
				defaultClassVars = DHTConstants.defaultDefaultClassVars;
			}
		}
		// FUTURE - allow for a real retention interval
		// FUTURE - eliminate parse
		String	opOptions;
		String	commonNSOptions;
		String	opTimeoutController;
		String	dirNSPutTimeoutController;
		String	dirNSOpOptions;
		String	dirNSValueRetentionPolicy;
		String	fileBlockNSValueRetentionPolicy;
		String	dirNSSSOptions;
		
		commonNSOptions = "revisionMode=NO_REVISIONS,storageType=FILE,consistencyProtocol="+ ConsistencyProtocol.TWO_PHASE_COMMIT;
		opTimeoutController = "opTimeoutController="+ options.opTimeoutController;
		dirNSPutTimeoutController = "opTimeoutController="+ options.dirNSPutTimeoutController;
		opOptions = "defaultPutOptions={compression="+ options.compression +",checksumType=MURMUR3_32,checksumCompressedValues=false,version=0,"+ opTimeoutController +"},"
				   +"defaultInvalidationOptions={"+ opTimeoutController +"},"
				   +"defaultGetOptions={nonExistenceResponse=NULL_VALUE,"+ opTimeoutController +"}";
		dirNSOpOptions = "defaultPutOptions={compression="+ options.compression +",checksumType=MURMUR3_32,checksumCompressedValues=false,version=0,"+ dirNSPutTimeoutController +"},"
				   +"defaultInvalidationOptions={"+ opTimeoutController +"},"
				   +"defaultGetOptions={nonExistenceResponse=NULL_VALUE,"+ opTimeoutController +"}";
		dirNSSSOptions = ",namespaceServerSideCode={putTrigger=com.ms.silverking.cloud.skfs.dir.serverside.DirectoryServer,retrieveTrigger=com.ms.silverking.cloud.skfs.dir.serverside.DirectoryServer}";
		//dirNSSSOptions = "";

		dirNSValueRetentionPolicy = "valueRetentionPolicy=<TimeAndVersionRetentionPolicy>{mode=wallClock,minVersions=1,timeSpanSeconds=86400}";
		fileBlockNSValueRetentionPolicy = options.fileBlockNSValueRetentionPolicy != null ? ","+ options.fileBlockNSValueRetentionPolicy : "";
		skfsNSOptions = NamespaceOptions.parse("versionMode=SINGLE_VERSION,"+ commonNSOptions +","+ opOptions);
		skfsMutableNSOptions =                   NamespaceOptions.parse("versionMode=SYSTEM_TIME_NANOS,"+ commonNSOptions +","+ opOptions);
		skfsFileBlockNSOptions =                 NamespaceOptions.parse("versionMode=SYSTEM_TIME_NANOS,"+ commonNSOptions +","+ opOptions + fileBlockNSValueRetentionPolicy);
		System.out.println(skfsFileBlockNSOptions);
		skfsDirNSOptions =                 		NamespaceOptions.parse("versionMode=SYSTEM_TIME_NANOS,"+ commonNSOptions +","+ dirNSOpOptions +","+ dirNSValueRetentionPolicy + dirNSSSOptions);
		skGlobalCodebase = PropertiesHelper.envHelper.getString("skGlobalCodebase", UndefinedAction.ZeroOnUndefined);
		
	}
	
	private static Pair<RingConfiguration,InstantiatedRingTree> getRing(DHTConfiguration dhtConfig, com.ms.silverking.cloud.dht.meta.MetaClient dhtMC) throws IOException, KeeperException {
		String ringName; 
		Pair<Long,Long>	ringVersion;
        DHTRingCurTargetZK	dhtRingCurTargetZK;
        Triple<String,Long,Long>	curRingNameAndVersion;
        RingConfiguration	ringConfig;
        RingConfigurationZK	ringConfigZK;
        InstantiatedRingTree	ringTree;
		com.ms.silverking.cloud.toporing.meta.MetaClient	ringMC;
        
        dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMC, dhtConfig);
        curRingNameAndVersion = dhtRingCurTargetZK.getCurRingAndVersionPair();
        if (curRingNameAndVersion == null) {
        	return null;
        }
        ringName = curRingNameAndVersion.getHead();
        ringVersion = curRingNameAndVersion.getTail();

        ringMC = com.ms.silverking.cloud.toporing.meta.MetaClient.createMetaClient(ringName, ringVersion.getV1(), dhtMC.getZooKeeper().getZKConfig()); 
        ringConfigZK = new RingConfigurationZK(ringMC);
        ringConfig = ringConfigZK.readFromZK(ringVersion.getV1(), null);
        
		//ringTree = SingleRingZK.readTree(ringMC, ringVersion);
        ringTree = null; // FUTURE - think about whether or not we should keep this, currently unused
        
        return new Pair<>(ringConfig, ringTree);
	}
	
	/*
	private static Map<String,HostGroupTable> getHostGroupTables(Set<String> hostGroupNames, ZooKeeperConfig zkConfig) throws KeeperException, IOException {
		Map<String,HostGroupTable>	hostGroupTables;
		
		hostGroupTables = new HashMap<>();
		for (String hostGroupName : hostGroupNames) {
			hostGroupTables.put(hostGroupName, getHostGroupTable(hostGroupName, zkConfig));
		}
		return hostGroupTables;
	}
	*/
	
	private static HostGroupTable getHostGroupTable(String hostGroupTableName, ZooKeeperConfig zkConfig) throws KeeperException, IOException {
		HostGroupTableZK	hostGroupTableZK;
		com.ms.silverking.cloud.meta.MetaClient	cloudMC;
		
		cloudMC = new com.ms.silverking.cloud.meta.MetaClient(CloudConfiguration.emptyTemplate.hostGroupTableName(hostGroupTableName), zkConfig);
		hostGroupTableZK = new HostGroupTableZK(cloudMC);		
		return hostGroupTableZK.readFromZK(-1, null);
	}
	
	private Set<String> findValidPassiveServers(Set<String> passiveNodeHostGroupNames, HostGroupTable hostGroupTable) {
		ImmutableSet.Builder<String>	validServers;
		
		validServers = ImmutableSet.builder();
		for (String hostGroupName : passiveNodeHostGroupNames) {
			validServers.addAll(hostGroupTable.getHostAddresses(hostGroupName));
		}
		return validServers.build();
	}

	/**
	 * Find all servers that are in the specified host groups, have class vars, and are in the ring
	 * @param hostGroupNames
	 * @param hostGroupTable
	 * @param ringTree
	 * @return
	 * @throws KeeperException 
	 */
	private Set<String> findValidActiveServers(Set<String> hostGroupNames, HostGroupTable hostGroupTable, 
											   InstantiatedRingTree ringTree) throws KeeperException {
		ImmutableSet.Builder<String>	validServers;
		Set<String>	candidateServers;
		
		candidateServers = new HashSet<>();
		for (String hostGroupName : hostGroupNames) {
			candidateServers.addAll(hostGroupTable.getHostAddresses(hostGroupName));
		}
		
		// FIXME - think about below
		// do we want to run on all servers or not?
		//candidateServers.retainAll(getAllServersInTree(ringTree));
		validServers = ImmutableSet.builder();
		validServers.addAll(candidateServers);
		return validServers.build();
	}
	
	private String findArbitraryActiveServer(Set<String> hostGroupNames, HostGroupTable hostGroupTable) {
		if (hostGroupNames.isEmpty()) {
			throw new RuntimeException("hostGroupNames is empty");
		} else {
			for (String hostGroupName : hostGroupNames) {
				Set<String>	servers;
				
				servers = hostGroupTable.getHostAddresses(hostGroupName);
				if (!servers.isEmpty()) {
					return servers.iterator().next();
				}
			}
			throw new RuntimeException("All HostGroups are empty");
		}
	}
	
	public Set<String> getAllServersInTree(InstantiatedRingTree ringTree) {
		Set<Node>	nodes;
		ImmutableSet.Builder<String>	servers;

		nodes = ringTree.getMemberNodes(OwnerQueryMode.All, NodeClass.server);
		servers = ImmutableSet.builder();
		for (Node node : nodes) {
			servers.add(node.getIDString());
		}
		return servers.build();
	}
	
	private String getJavaCmdStart(SKAdminOptions options, ClassVars classVars) {		
		return options.javaBinary +" -cp "+ options.classPath
				+" "+ options.assertionOption 
				+" "+ getJVMOptions(classVars)
				+" "+ getProfilingOptions(options)
				+" "+ getJVMMemoryOptions(classVars)
				+" "+ getDHTOptions(options, classVars);
	}
	
	private String getJVMOptions(ClassVars classVars) {
		return "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath="
				+getHeapDumpFile(classVars);
	}
	
	private String getHeapDumpFile(ClassVars classVars) {
		return DHTConstants.getSKInstanceLogDir(classVars, gc) +"/"+ DHTConstants.heapDumpFile;
	}

	private String getProfilingOptions(SKAdminOptions options) {
		return options.profilingOptions;
	}
	
	private String getJVMMemoryOptions(ClassVars classVars) {
		Pair<String,String>	heapLimits;
		
		heapLimits = getHeapLimits(classVars);
		return "-Xms"+ heapLimits.getV1() +" -Xmx"+ heapLimits.getV2();
	}
	
	private String getReapInterval(ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.reapIntervalVar);
	}
	
	private String getFileSegmentCacheCapacity(ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.fileSegmentCacheCapacityVar);
	}
	
	private String getRetrievalImplementation(ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.retrievalImplementationVar);
	}
	
	private String getSegmentIndexLocation(ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.segmentIndexLocationVar);
	}
	
	private String getNSPrereadGB(ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.nsPrereadGBVar);
	}
	
	private String getDHTOptions(SKAdminOptions options, ClassVars classVars) {
		return "-Dcom.ms.silverking.Log="+ options.logLevel
				+" -D"+ DHTConstants.dataBasePathProperty +"="+ getDataDir(classVars)
				+" -D"+ DHTConstants.reapIntervalProperty +"="+ getReapInterval(classVars)
				+" -D"+ DHTConstants.fileSegmentCacheCapacityProperty +"="+ getFileSegmentCacheCapacity(classVars)
				+" -D"+ DHTConstants.retrievalImplementationProperty +"="+ getRetrievalImplementation(classVars)
				+" -D"+ DHTConstants.segmentIndexLocationProperty +"="+ getSegmentIndexLocation(classVars)
				+" -D"+ DHTConstants.nsPrereadGBProperty +"="+ getNSPrereadGB(classVars)
				;
	}
	
	private String createStartCommand(DHTConfiguration dhtConfig, ClassVars classVars, SKAdminOptions options) {
		String	cmdFile;
		
		cmdFile = "/tmp/cmd."+ System.currentTimeMillis();
		return "echo \""+ _createStartCommand(dhtConfig, classVars, options) +"\" > "+ cmdFile +"; chmod +x "+ cmdFile +"; nohup "+cmdFile +" 1> /dev/null 2>&1 < /dev/null &";
	}
	
	// FUTURE - make os specific commands generic
	private String _createStartCommand(DHTConfiguration dhtConfig, ClassVars classVars, SKAdminOptions options) {
		String	daemonLogDir;
		String	daemonLogFile;
		String	prevDaemonLogFile;
		boolean	destructive;
		
		destructive = options.destructive;
		daemonLogDir = DHTConstants.getSKInstanceLogDir(classVars, gc);
		daemonLogFile = daemonLogDir +"/"+ DHTConstants.daemonLogFile;
		prevDaemonLogFile = daemonLogDir +"/"+ DHTConstants.prevDaemonLogFile;
		return  (destructive ? "" : "netstat -tulpn | grep tcp.*:"+ dhtConfig.getPort() +" ; ") +
				(destructive ? "" : "if [ \\$? -ne 0 ]; then { ") +
				(destructive ? createStopCommand(dhtConfig, classVars) +"; " : "")
				+"mkdir -p "+ getDataDir(classVars) +"; "
				+"mkdir -p "+ daemonLogDir +"; "
				+"rm "+ getHeapDumpFile(classVars) +"; "
				+"mv "+ daemonLogFile +" "+ prevDaemonLogFile +"; "
				+ getPreJavaCommand(classVars) +" "
				+ getNodeEnv(classVars)
				+ getTaskset(options)
				+ getJavaCmdStart(options, classVars) 
				+" "+ DHTNode.class.getCanonicalName()
				+ (options.disableReap ? " -r " : "")
				+ (options.leaveTrash ? " -leaveTrash " : "")
				+" -n "+ gc.getClientDHTConfiguration().getName() 
				+" -z "+ gc.getClientDHTConfiguration().getZKConfig()
				+" -into "+ options.inactiveNodeTimeoutSeconds
				+( destructive ? (" 1>"+ daemonLogFile +" 2>&1 &") : (" 1>"+ daemonLogFile +"; 2>&1; } & fi") )
				;
	}
	
	private String getTaskset(SKAdminOptions options) {
		if (options.pinToNICLocalCPUs != null) {
			String	nic;
			
			nic = options.pinToNICLocalCPUs;
			/*
			try {
				return ProcessUtil.getPinCPUCommandFromCPUList(NICUtil.getLocalCPUList(options.pinToNICLocalCPUs)) +" ";
			} catch (IOException ioe) {
				Log.logErrorWarning(ioe, "Unable to pin CPUs. Ignoring pin.");
			}
			*/
			return "taskset -c `cat /sys/class/net/"+ nic +"/device/local_cpulist` ";
			//return "export cpuList=`cat /sys/class/net/"+ nic +"/device/local_cpulist`; "
			//		+"taskset -c $cpuList ";
		}
		return "";
	}

	private String getNodeEnv(ClassVars classVars) {
		String	ipAliasMapFile;
		
		ipAliasMapFile = classVars.getVarMap().get(DHTConstants.ipAliasMapFileVar);
		if (ipAliasMapFile != null && ipAliasMapFile.trim().length() > 0) {
			return "export "+ DHTConstants.ipAliasMapFileEnvVar +"="+ ipAliasMapFile.trim() +"; ";
		} else {
			return "";
		}
	}

	private String createStopCommand(DHTConfiguration dhtConfig, ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.killCommandVar) +" "+ gc.getClientDHTConfiguration().getName();
	}
	
	private String createCheckSKFSCommand(DHTConfiguration dhtConfig, ClassVars classVars) {
		String	logDir;
		String	logFile;
		
		logDir = DHTConstants.getSKInstanceLogDir(classVars, gc);
		logFile = logDir +"/"+ DHTConstants.checkSKFSLogFile;		
		return  "mkdir -p "+ logDir +"; "
				+getCheckSKFSBaseCommand(classVars, "CheckSKFS")
				+" -C "+ options.compression
				+" -l "+ options.logLevel
				+(options.coreLimit == null ? "" : " -L "+ options.coreLimit)
				//+" -n "+ options.fsNativeOnlyFile
				+" 1>"+ logFile +" 2>&1 &";
	}
	
	private String createStopSKFSCommand(DHTConfiguration dhtConfig, ClassVars classVars) {
		String	logDir;
		String	logFile;
		
		logDir = DHTConstants.getSKInstanceLogDir(classVars, gc);
		logFile = logDir +"/"+ DHTConstants.stopSKFSLogFile;		
		return "mkdir -p "+ logDir +"; "
				+getCheckSKFSBaseCommand(classVars, "StopSKFS")
				+" 1>"+ logFile +" 2>&1 &";
	}
	
	private String createClearDataCommand(DHTConfiguration dhtConfig, ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.clearDataCommandVar) +" "+ getDataDir(classVars);
	}
	
	private String getPreJavaCommand(ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.skDaemonJavaCommandHeaderVar);
	}
	
	private Pair<String,String> getHeapLimits(ClassVars classVars) {
		String	initialHeapSize;
		String	maxHeapSize;
		
		initialHeapSize = checkForLegacyHeapDef(classVars.getVarMap().get(DHTConstants.initialHeapSizeVar));
		maxHeapSize = checkForLegacyHeapDef(classVars.getVarMap().get(DHTConstants.maxHeapSizeVar));
		return new Pair<>(initialHeapSize, maxHeapSize);
	}
	
	/*
	 * Previous heap definitions allowed unit-less quantities to be treated as MB. 
	 * Do that here for smaller unit-less quantities
	 */
	private String checkForLegacyHeapDef(String def) {
		def = def.trim();
		if (Character.isDigit(def.charAt(def.length() - 1))) {
			Long	val;
			
			val = Long.parseLong(def);
			if (val < 1024 * 1024) {
				def = def +"M";
			}
		}
		return def;
	}
	
	private String getDataDir(ClassVars classVars) {
		return classVars.getVarMap().get(DHTConstants.dataBaseVar)
					+"/"+ gc.getClientDHTConfiguration().getName();
	}
	
	private String getCheckSKFSBaseCommand(ClassVars classVars, String command) {
		return (skGlobalCodebase == null ? "" : "export skGlobalCodebase="+ skGlobalCodebase +"; ")
				+"export "+ GridConfiguration.defaultBaseEnvVar +"="+ 
					(options.gridConfigBase == null ? GridConfiguration.getDefaultBase() : new File(options.gridConfigBase)) +"; " 
				+"export "+ DHTConstants.jaceHomeEnv +"="+ PropertiesHelper.envHelper.getString(DHTConstants.jaceHomeEnv, UndefinedAction.ExceptionOnUndefined) +"; "
				+"export "+ DHTConstants.javaHomeEnv +"="+ PropertiesHelper.envHelper.getString(DHTConstants.javaHomeEnv, getSystemJavaHome()) +"; "
				+"export "+ DHTConstants.classpathEnv +"="+ PropertiesHelper.envHelper.getString(DHTConstants.classpathEnv, System.getProperty(DHTConstants.classpathProperty)) +"; "
				+getTaskset(options)
				+classVars.getVarMap().get(DHTConstants.checkSKFSCommandVar)
					+" -g "+ gc.getName() +" -z "+ gc.getClientDHTConfiguration().getZKConfig()
					+" -c "+ command +" -f "+ options.forceSKFSDirectoryCreation
					+ skfsTimeoutOption("skfsEntryTimeoutSecs", options.skfsEntryTimeoutSecs)
					+ skfsTimeoutOption("skfsAttrTimeoutSecs", options.skfsAttrTimeoutSecs)
					+ skfsTimeoutOption("skfsNegativeTimeoutSecs", options.skfsNegativeTimeoutSecs);
	}
	
	
	
	private String skfsTimeoutOption(String name, int timeout) {
		if (timeout == SKAdminOptions.skfsTimeoutNotSet) {
			return "";
		} else {
			String	ch;
			
			switch (name) {
			case "skfsEntryTimeoutSecs": ch = "E"; break;
			case "skfsAttrTimeoutSecs": ch = "A"; break;
			case "skfsNegativeTimeoutSecs": ch = "N"; break;
			default: throw new RuntimeException("panic");
			}
			
			return " -"+ ch +" "+ timeout;
		}
	}

	private String getSystemJavaHome() {
		String	jhProp;
		
		jhProp = System.getProperty(DHTConstants.javaHomeProperty);
		if (jhProp.endsWith(jreSuffix)) {
			jhProp = jhProp.substring(0, jhProp.length() - jreSuffix.length());
		}
		return jhProp;
	}

	public boolean execCommand(SKAdminCommand[] commands) throws IOException, KeeperException, ClientException {
		boolean	result;
		SKAdminCommand[][]	commandGroups;
		
		commandGroups = createCommandGroups(commands);
		result = true;
		for (SKAdminCommand[] commandGroup : commandGroups) {
			boolean	_result;
			
			if (commandGroup[0].isClusterCommand()) {
				_result = execClusterCommandGroup(commandGroup);
			} else {
				_result = execAdminCommandGroup(commandGroup);
			}
			if (!_result) {
				return false;
			}
		}
		return result;
	}

	/**
	 * Split commands into contiguous groups of cluster or admin commands.
	 * Each group is of only one type. The order of the original array
	 * is maintained.
	 * @param commands list of commands
	 * @return commands grouped by command type
	 */
	private SKAdminCommand[][] createCommandGroups(SKAdminCommand[] commands) {
		List<List<SKAdminCommand>>	_commandGroups;
		List<SKAdminCommand>	_currentGroup;
		SKAdminCommand[][]	commandGroups;
		
		_commandGroups = new ArrayList<>();
		_currentGroup = new ArrayList<>();
		for (SKAdminCommand command : commands) {
			if (_currentGroup.size() == 0) {
				_currentGroup.add(command);
			} else {
				if (_currentGroup.get(0).isClusterCommand() == command.isClusterCommand()) {
					_currentGroup.add(command);
				} else {
					_commandGroups.add(_currentGroup);
					_currentGroup = new ArrayList<>();
					_currentGroup.add(command);
				}
			}
		}
		if (_currentGroup.size() > 0) {
			_commandGroups.add(_currentGroup);
		}
		_currentGroup = null;
		
		commandGroups = new SKAdminCommand[_commandGroups.size()][];
		for (int i = 0; i < _commandGroups.size(); i++) {
			commandGroups[i] = _commandGroups.get(i).toArray(new SKAdminCommand[0]);
		}
		return commandGroups;
	}

	private boolean execAdminCommandGroup(SKAdminCommand[] commands) throws IOException, KeeperException, ClientException {
		boolean	result;
		
		result = true;
		for (SKAdminCommand command : commands) {
			boolean	_result;
			
			if (!options.displayOnly) {
				Log.warning("Executing admin command: ", command);
				switch (command) {
				case CreateSKFSns:
					_result = execCreateSKFSns();
					break;
				case ClearInstanceExclusions:
					_result = clearInstanceExclusions();
					break;
				case SetInstanceExclusions:
					_result = setInstanceExclusions();
					break;
				case GetInstanceExclusions:
					_result = displayInstanceExclusions();
					break;
				case GetActiveDaemons:
					_result = getActiveDaemons();
					break;
				case EnsureNoActiveDaemons:
					_result = ensureNoActiveDaemons();
					break;
				default:
					throw new RuntimeException("panic");
				}
				result = result && _result;
			} else {
				Log.warning("Admin command: ", command);
			}
		}
		return result;
	}
	
	private boolean clearInstanceExclusions() throws KeeperException, IOException {
		InstanceExclusionZK	instanceExclusionZK;
		
		instanceExclusionZK = new InstanceExclusionZK(dhtMC);
		instanceExclusionZK.writeToZK(ExclusionSet.emptyExclusionSet(0));
		return true;
	}

	private boolean setInstanceExclusions() throws KeeperException, IOException {
		return setInstanceExclusions(options.targets != null ? ExclusionSet.parse(options.targets) : ExclusionSet.emptyExclusionSet(VersionedDefinition.NO_VERSION));
	}
	
	private boolean setInstanceExclusions(ExclusionSet exclusionSet) throws KeeperException, IOException {
		InstanceExclusionZK	instanceExclusionZK;
		
		instanceExclusionZK = new InstanceExclusionZK(dhtMC);
		instanceExclusionZK.writeToZK(exclusionSet);
		return true;
	}
	
	private boolean displayInstanceExclusions() throws KeeperException, IOException {
		System.out.println(getInstanceExclusions());
		return true;
	}
	
	private ExclusionSet getInstanceExclusions() throws KeeperException, IOException {
		InstanceExclusionZK	instanceExclusionZK;
		ExclusionSet		exclusions;
		
		instanceExclusionZK = new InstanceExclusionZK(dhtMC);
		exclusions = instanceExclusionZK.readFromZK(VersionedDefinition.NO_VERSION, null);
		return exclusions;
	}
	
	private void addToInstanceExclusions(Set<IPAndPort> serversToAdd) throws KeeperException, IOException {
		ExclusionSet	exclusionSet;
		ExclusionSet	newExclusionSet;
		
		exclusionSet = getInstanceExclusions();
		newExclusionSet = exclusionSet.addByIPAndPort(serversToAdd);
		setInstanceExclusions(newExclusionSet);
		System.out.println("............................");
		System.out.println(newExclusionSet);
		System.out.println("----------------------------");
		displayInstanceExclusions();
		System.out.println("============================");
	}
	
	private Pair<Boolean,Set<IPAndPort>> _getActiveDaemons() {
		try {
			Set<IPAndPort>	activeDaemons;
			
			activeDaemons = suspectsZK.readActiveNodesFromZK();
			for (IPAndPort daemon : activeDaemons) {
				System.out.printf("%s\n", daemon.getIPAsString());
			}
			return new Pair<>(true, activeDaemons);
		} catch (KeeperException ke) {
			Log.logErrorWarning(ke);
			return new Pair(false, ImmutableSet.of());
		}
	}
	
	private boolean getActiveDaemons() {
		return _getActiveDaemons().getV1();
	}
	
	private boolean ensureNoActiveDaemons() {
		Pair<Boolean,Set<IPAndPort>>	result;
		
		result = _getActiveDaemons();
		return result.getV1() && result.getV2().size() == 0;
	}
	
	private boolean execCreateSKFSns() throws IOException, ClientException, KeeperException {
		SKFSNamespaceCreator	nsCreator;
		String					preferredServer;
		Pair<RingConfiguration,InstantiatedRingTree>	ringConfigAndTree;
		HostGroupTable			hostGroupTable;
		String					hostGroupTableName;
		
		ringConfigAndTree = getRing(dhtConfig, dhtMC);
		Log.warning("ringConfig: ", ringConfigAndTree.getV1());
		hostGroupTableName = ringConfigAndTree.getV1().getCloudConfiguration().getHostGroupTableName();
		Log.warning("hostGroupTableName: ", hostGroupTableName);
		hostGroupTable = getHostGroupTable(hostGroupTableName, dhtMC.getZooKeeper().getZKConfig());
		
		if (options.preferredServer == null) {
			preferredServer = findArbitraryActiveServer(dhtConfig.getHostGroups(), hostGroupTable);
		} else {
			preferredServer = options.preferredServer;
		}
		Log.warning("Using preferredServer ", preferredServer);
		nsCreator = new SKFSNamespaceCreator(gc.getClientDHTConfiguration(), preferredServer);
		nsCreator.createNamespaces(skfsNamespaces, skfsNSOptions);
		nsCreator.createNamespaces(skfsMutableNamespaces, skfsMutableNSOptions);
		nsCreator.createNamespaces(skfsFileBlockNamespaces, skfsFileBlockNSOptions);
		nsCreator.createNamespaces(skfsDirNamespaces, skfsDirNSOptions);
		return true;
	}
	
	private Set<String> retainOnlySpecifiedAndNonExcludedServers(Set<String> servers, Set<String> targetServers) {
		Set<String>	_servers;
		
		_servers = new HashSet<>(servers);
		if (!options.includeExcludedHosts && !options.targetsEqualsExclusionsTarget() && ! options.targetsEqualsActiveDaemonsTarget()) {
			_servers.removeAll(exclusionSet.getServers());
		}
		if (options.targets != null) {
			Set<String>	_s;
			
			_s = new HashSet<>(_servers);
			if (options.targetsEqualsExclusionsTarget()) {
				_s.retainAll(exclusionSet.getServers());
			} else {
				if (options.targetsEqualsActiveDaemonsTarget()) {
					_s = IPAndPort.copyServerIPsAsMutableSet(_getActiveDaemons().getV2());
				} else {
					_s.retainAll(targetServers);
				}
			}
			return _s;
		} else {
			return _servers;
		}		
	}
	
	private void verifyServerEligibility(Set<String> servers, SKAdminCommand[] commands) throws KeeperException {
		if (Arrays.contains(commands, SKAdminCommand.StartNodes)) {
			Set<String>			candidateServers;
			Set<String>			ineligibleServers;
			DHTRingCurTargetZK	dhtRingCurTargetZK;
			InstanceExclusionZK	instanceExclusionZK;
			
			dhtRingCurTargetZK = new DHTRingCurTargetZK(dhtMC, dhtMC.getDHTConfiguration());
			instanceExclusionZK = new InstanceExclusionZK(dhtMC);
			Log.warning("Verifying server eligibility for start");
			candidateServers = new HashSet<>(servers);
			ineligibleServers = HealthMonitor.removeIneligibleServers(candidateServers, dhtRingCurTargetZK, instanceExclusionZK);
			if (ineligibleServers.size() > 0) {
				for (String ineligibleServer : ineligibleServers) {
					Log.warning("Ineligible server: ", ineligibleServer);
				}
				if (!options.forceInclusionOfUnsafeExcludedServers) {
					throw new IneligibleServerException("Attempted to start ineligible servers: "+ CollectionUtil.toString(ineligibleServers, ','));
				} else {
					Log.countdownWarning("*** Including unsafe excluded servers. This may result in data loss ***", unsafeWarningCountdown);
				}
			} else {
				Log.warning("Server eligibility verified");
			}
		}		
	}	
	
    public InstantiatedRingTree readCurrentTree() throws KeeperException, IOException {
    	DHTRingCurTargetZK	curTargetZK;
    	Triple<String,Long,Long>	curRingAndVersionPair;
    	InstantiatedRingTree	ringTree;
    	
    	curTargetZK = new DHTRingCurTargetZK(dhtMC, dhtConfig);
    	curRingAndVersionPair = curTargetZK.getCurRingAndVersionPair();
    	ringTree = SingleRingZK.readTree(new com.ms.silverking.cloud.toporing.meta.MetaClient(
    			new NamedRingConfiguration(dhtConfig.getRingName(), ringConfig), dhtMC.getZooKeeper().getZKConfig()), curRingAndVersionPair.getTail());
    	return ringTree;
    }
    

	private boolean execClusterCommandGroup(SKAdminCommand[] commands) throws IOException, KeeperException {
		/*
		 * Each DHT consists of active + passive nodes
		 * Filter active&passive by particular host groups
		 * 	any servers that aren't in the included host groups won't be used
		 * Fetch all host group tables
		 * Fetch all class variables for the host groups
		 * For ChecSKFS, fetch the skfs environment
		 * Wait for all fetches to complete
		 * Create map of servers->commands to run
		 * Pass the command map to TwoLevelParallelSSH and run
		 * Run/wait until complete
		 */
		//Map<String,HostGroupTable>	hostGroupTables;
		//hostGroupTables = getHostGroupTables(hostGroups, dhtMC.getZooKeeper().getZKConfig());
		
		Set<String>				activeHostGroupNames;
		Map<String,ClassVars>	hostGroupToClassVars;
		HostGroupTable			hostGroupTable;
		Set<String>				validActiveServers;
		Set<String>				validPassiveServers;
		String					hostGroupTableName;
		Map<String,String[]>	serverCommands;
		Set<String>				passiveNodeHostGroupNames;
		boolean					result;
		Set<String>				targetServers;
		Set<String>				passiveTargetServers;
		
		targetServers = CollectionUtil.parseSet(options.targets, ",");		
		
		activeHostGroupNames = dhtConfig.getHostGroups();
		Log.warning("hostGroupNames: ", CollectionUtil.toString(activeHostGroupNames));		
		hostGroupToClassVars = getHostGroupToClassVarsMap(dhtConfig);
		Log.warning("hostGroupToClassVars: ", CollectionUtil.mapToString(hostGroupToClassVars));
		Log.warning("ringConfig: ", ringConfig);
		hostGroupTableName = ringConfig.getCloudConfiguration().getHostGroupTableName();
		Log.warning("hostGroupTableName: ", hostGroupTableName);
		hostGroupTable = getHostGroupTable(hostGroupTableName, dhtMC.getZooKeeper().getZKConfig());
		
		// FUTURE - Do more validation of configuration. E.g. prevent a server from being both
		// active and passive, the ring from containing servers without class vars, etc.
		
		validActiveServers = findValidActiveServers(activeHostGroupNames, hostGroupTable, ringTree);
		validActiveServers = retainOnlySpecifiedAndNonExcludedServers(validActiveServers, targetServers);
		verifyServerEligibility(validActiveServers, commands);
		Log.warning("validActiveServers: ", CollectionUtil.toString(validActiveServers));
		
		// Allow StopNodes with empty validActiveServers if the target is activeDaemons
		if (options.targetsEqualsActiveDaemonsTarget() && validActiveServers.isEmpty()) {
			boolean	exitOK;
			
			exitOK = true;
			for (SKAdminCommand command : commands) {
				if (command != SKAdminCommand.StopNodes) {
					exitOK = false;
				}
			}
			if (exitOK) {
				return true;
			}
		}
		
		passiveTargetServers = new HashSet<>();
		passiveTargetServers.addAll(targetServers);
		passiveTargetServers.removeAll(validActiveServers);
		
		passiveNodeHostGroupNames = dhtConfig.getPassiveNodeHostGroupsAsSet();
		Log.warning("passiveNodeHostGroupNames: ", CollectionUtil.toString(passiveNodeHostGroupNames));
		
		if (passiveTargetServers.size() > 0) {
			validPassiveServers = ImmutableSet.copyOf(passiveTargetServers);
		} else {
			validPassiveServers = findValidPassiveServers(passiveNodeHostGroupNames, hostGroupTable);
		}
		validPassiveServers = retainOnlySpecifiedAndNonExcludedServers(validPassiveServers, passiveTargetServers);
		Log.warning("validPassiveServers: ", CollectionUtil.toString(validPassiveServers));
		
		if (Arrays.contains(commands, SKAdminCommand.ClearData) && !options.targetsEqualsExclusionsTarget()) {
			Log.countdownWarning("*** Clearing ALL data ***", unsafeWarningCountdown);
		}
		
		result = true;
		for (SKAdminCommand command : commands) {
			boolean	_result;
			
			Log.warning("Executing cluster command: ", command);			
			serverCommands = createServerCommands(command, validActiveServers, validPassiveServers, 
												hostGroupTable, hostGroupToClassVars, 
												activeHostGroupNames, passiveNodeHostGroupNames);
			displayCommandMap(serverCommands);
			if (!options.displayOnly) {
				_result = execCommandMap(serverCommands, validActiveServers.size() > 0 ? validActiveServers : validPassiveServers, hostGroupTable);
				result = result && _result;
				if (!result) {
					break;
				}
			}
			if (command.equals(SKAdminCommand.StartNodes)) {
				int[]	timeouts;
				boolean	running;
				int		attemptIndex;
				
				Log.warning("Waiting for nodes to enter running state...");
				timeouts = NumUtil.parseIntArray(options.timeoutSeconds, ",");
				running = false;
				attemptIndex = 0;
				do {
					Pair<Set<IPAndPort>,Boolean>	waitResult;
					Set<IPAndPort>	failedServers;
					
					Log.warningf("attemptIndex: %d\ttimeout: %d", attemptIndex, timeouts[attemptIndex]);					

					if (replicaSetExcludedByExclusions(exclusionSet)) {
						return false;
					}
					
					waitResult = waitUntilRunning(IPAndPort.set(validActiveServers, dhtConfig.getPort()), timeouts[attemptIndex]);
					failedServers = waitResult.getV1();
					if (waitResult.getV2()) {
						running = true;
					} else {
						++attemptIndex;
						if (attemptIndex < timeouts.length) {
							Log.warningf("Adding to instance exclusion set: %s", failedServers);
							if (options.excludeInstanceExclusions) {
								exclusionSet = exclusionSet.addByIPAndPort(failedServers);
							}
							addToInstanceExclusions(failedServers);
							validActiveServers = removeServers(validActiveServers, failedServers);
						}
					}
				} while (!running && attemptIndex < timeouts.length);
				if (!running) {
					return false;
				}
			}
		}
		return result;
	}
	
	private boolean replicaSetExcludedByExclusions(ExclusionSet es) throws KeeperException, IOException {
		InstantiatedRingTree	curTree;
		ResolvedReplicaMap		replicaMap;
		List<Set<IPAndPort>>	excludedReplicaSets;
		
		curTree = readCurrentTree();
		replicaMap = curTree.getResolvedMap(ringConfig.getRingParentName(), new ReplicaNaiveIPPrioritizer());
		excludedReplicaSets = replicaMap.getExcludedReplicaSets(es.asIPAndPortSet(0));
		if (excludedReplicaSets.size() != 0) {
			Log.warning("Exclusion set excludes at least one replica set:");
			for (Set<IPAndPort> s : excludedReplicaSets) {
				Log.warningf("%s", s);
			}
			return true;
		}
		return false;
	}
	
	private Set<String> removeServers(Set<String> originalServers, Set<IPAndPort> serversToRemove) {
		Set<String>	newServers;
		
		newServers = new HashSet<>();
		newServers.addAll(originalServers);
		newServers.removeAll(IPAndPort.copyServerIPsAsMutableSet(serversToRemove));
		return ImmutableSet.copyOf(newServers);
	}

	private boolean execCommandMap(Map<String, String[]> serverCommands, Set<String> workerCandidateHosts, HostGroupTable hostGroups) throws IOException {
		TwoLevelParallelSSHMaster	sshMaster;
		boolean	result;
		
		Log.warningf("serverCommands.size() %d", serverCommands.size());
		sshMaster = new TwoLevelParallelSSHMaster(serverCommands, ImmutableList.copyOf(workerCandidateHosts), options.numWorkerThreads, options.workerTimeoutSeconds, options.maxAttempts, false);
		Log.warning("Starting workers");
		sshMaster.startWorkers(hostGroups);
		Log.warning("Waiting for workers");
		result = sshMaster.waitForWorkerCompletion();
		sshMaster.terminate();
		Log.warning("Workers complete");
		return result;
	}

	private Map<String, ClassVars> getHostGroupToClassVarsMap(DHTConfiguration dhtConfig) throws KeeperException {
		Map<String,String>	hostGroupToClassVarNames;
		Map<String,ClassVars>	hostGroupToClassVars;
		
		hostGroupToClassVars = new HashMap<>();
		hostGroupToClassVarNames = dhtConfig.getHostGroupToClassVarsMap();
		for (Map.Entry<String,String> hostGroupAndClassVarsName : hostGroupToClassVarNames.entrySet()) {
			hostGroupToClassVars.put(hostGroupAndClassVarsName.getKey(), classVarsZK.getClassVars(hostGroupAndClassVarsName.getValue()));
		}
		return hostGroupToClassVars;
	}

	private Map<String,String[]> createServerCommands(SKAdminCommand command, Set<String> validActiveServers, 
										Set<String> validPassiveServers, HostGroupTable hostGroupTable, Map<String,ClassVars> hostGroupToClassVars,
										Set<String> activeHostGroupNames, Set<String> passiveNodeHostGroupNames) {
		Map<String,String[]>	serverCommands;
		Set<String>				allServers;
		
		allServers = new HashSet<>();
		
		if (command == SKAdminCommand.ClearInstanceExclusionsData) {
			try {
				ExclusionSet	e;
				
				e = getInstanceExclusions();					
				if (replicaSetExcludedByExclusions(e)) {
					Log.warning("Can't clear instance exclusions data. At least one replica set is entirely excluded.");
					throw new RuntimeException("Entire replica set excluded");
				} else {
					Log.warning("Servers to clear data from:\n", e);
					Log.countdownWarning("*** Clearing instance exclusions data ***", unsafeWarningCountdown);
					allServers.addAll(e.getServers());
				}
			} catch (KeeperException | IOException e) {
				throw new RuntimeException("Exception calling getInstanceExclusions()", e);
			}
		} else {
			allServers.addAll(validActiveServers);
			allServers.addAll(validPassiveServers);
		}
		
		serverCommands = new HashMap<>();
		for (String server : allServers) {
			String		rawServerCommand;
			String[]	serverCommand;
			ClassVars	serverClassVars;
			
			serverClassVars = getServerClassVars(server, hostGroupTable, activeHostGroupNames, passiveNodeHostGroupNames, hostGroupToClassVars);
			if (serverClassVars != null) {
				switch (command) {
				case StartNodes:
					rawServerCommand = createStartCommand(dhtConfig, serverClassVars, options);
					break;
				case StopNodes:
					rawServerCommand = createStopCommand(dhtConfig, serverClassVars);
					break;
				case ClearInstanceExclusionsData:
				case ClearData:
					rawServerCommand = createClearDataCommand(dhtConfig, serverClassVars);
					break;
				case StartSKFS:
					if (options.destructive) {
						throw new RuntimeException("Destructive StartSKFS not supported");
					}
				case CheckSKFS:
					rawServerCommand = createCheckSKFSCommand(dhtConfig, serverClassVars);
					break;
				case StopSKFS:
					rawServerCommand = createStopSKFSCommand(dhtConfig, serverClassVars);
					break;
				default:
					throw new RuntimeException("Unsupported command: "+ command);
				}
				serverCommand = rawServerCommand.split("\\s+");
				serverCommands.put(server, serverCommand);
			}
		}
		return serverCommands;
	}
	
	private void displayCommandMap(Map<String,String[]> cmdMap) {
		for (Map.Entry<String, String[]> cmdEntry : cmdMap.entrySet()) {
			System.out.printf("%s\t%s\n", cmdEntry.getKey(), ArrayUtil.toString(cmdEntry.getValue(), ' '));
		}
	}
	
	private ClassVars getServerClassVars(String server, HostGroupTable hostGroupTable, Set<String> activeHostGroupNames,
										 Set<String> passiveNodeHostGroupNames,
										 Map<String,ClassVars> hostGroupToClassVars) {
		Set<String>	serverHostGroups;
		Set<String>	acceptableHostGroups;
		String		serverHostGroup;
		Set<String>	activeAndPassiveHostGroupNames;
		ClassVars	serverClassVars;
		
		serverHostGroups = hostGroupTable.getHostGroups(server);
		acceptableHostGroups = new HashSet<>(serverHostGroups);
		activeAndPassiveHostGroupNames = new HashSet<>();
		activeAndPassiveHostGroupNames.addAll(activeHostGroupNames);
		activeAndPassiveHostGroupNames.addAll(passiveNodeHostGroupNames);
		acceptableHostGroups.retainAll(activeAndPassiveHostGroupNames);
		if (acceptableHostGroups.size() == 1) {
			serverHostGroup = acceptableHostGroups.iterator().next();
		} else if (acceptableHostGroups.size() > 1) {
			// FUTURE - Could select a "best". For now, just pick the first.
			Log.warning(server +" has more than one valid host group ");
			serverHostGroup = acceptableHostGroups.iterator().next();
		} else {
			Log.warning(server +" has no valid host group ");
			return defaultClassVars;
		}
		serverClassVars = hostGroupToClassVars.get(serverHostGroup);
		if (serverClassVars != null) {
			return defaultClassVars.overrideWith(serverClassVars);
		} else {
			Log.warning(server +" has no ClassVars for group "+ serverHostGroup);
			return defaultClassVars;
		}
	}
	
	///////////////////////////////////////////
	
	public Pair<Set<IPAndPort>,Boolean> waitUntilRunning(Set<IPAndPort> activeNodes, int timeoutSeconds) {
		DaemonStateZK				daemonStateZK;
		Map<IPAndPort, DaemonState>	daemonState;
		
        daemonStateZK = new DaemonStateZK(dhtMC);
        daemonState = daemonStateZK.waitForQuorumState(activeNodes, DaemonState.RUNNING, timeoutSeconds, true);
        Log.warningf("daemonState: %s", daemonState);
        if (daemonState.isEmpty()) {
        	return new Pair<>(ImmutableSet.of(), true);
        } else {
        	HashSet<IPAndPort>	failedDaemons;
        	boolean				running;

        	running = true;
        	failedDaemons = new HashSet<>();
        	for (IPAndPort activeNode : activeNodes) {
        		DaemonState	ds;
        		
        		ds = daemonState.get(activeNode);
        		System.out.printf("Node: %s\tstate: %s\n", activeNode, ds);
        		if (ds == null) {
        			failedDaemons.add(activeNode);
        			running = false;
        		} else {
        			if (ds != DaemonState.RUNNING) {
        				running = false;
        			}
        		}
        	}
        	return new Pair<>(failedDaemons, running);
        }
	}
	
	///////////////////////////////////////////

	private static void fillDefaultOptions(SKAdminOptions options) {
		if (options.classPath == null) {
			options.classPath = System.getProperty("java.class.path");
		}
		if (options.javaBinary == null) {
			options.javaBinary = System.getProperty("java.home") +"/bin/java";
		}
	}
	

	private static void sanityCheckOptions(SKAdminOptions options) {
		// FUTURE - add
	}
	
	///////////////////////////////////////////
	
	public static void main(String[] args) {
		boolean			success;
		
		success = false;
    	try {
    		SKAdmin			skAdmin;
    		SKAdminOptions	options;
    		CmdLineParser	parser;
    		SKGridConfiguration	gc;
    		SKAdminCommand[]	commands;
    		
            LWTPoolProvider.createDefaultWorkPools();
            LWTThreadUtil.setLWTThread();
    		options = new SKAdminOptions();
    		parser = new CmdLineParser(options);
    		try {
    			parser.parseArgument(args);
    		} catch (CmdLineException cle) {
    			System.err.println(cle.getMessage());
    			parser.printUsage(System.err);
                System.exit(-1);
    		}
    		
    		fillDefaultOptions(options);
    		sanityCheckOptions(options);
    		
    		if (options.gridConfigBase != null) {
    			gc = SKGridConfiguration.parseFile(new File(options.gridConfigBase), options.gridConfig);
    		} else {
    			gc = SKGridConfiguration.parseFile(options.gridConfig);
    		}
    		// For now, redirection is handled by the launching script. 
    		// We will probably move this to use below in the future.
    		//LogStreamConfig.configureLogStreams(gc, logFileName);
    		
    		if (options.forceInclusionOfUnsafeExcludedServers) {
				Log.countdownWarning("Options requesting unsafe excluded servers. This may result in data loss", unsafeWarningCountdown);
    		}
    		
    		skAdmin = new SKAdmin(gc, options);
    		commands = SKAdminCommand.parseCommands(options.commands);
    		success = skAdmin.execCommand(commands);
    	} catch (IneligibleServerException ise) {
    		throw ise;
    	} catch (Exception e) {
    		e.printStackTrace();
            System.exit(-1);
    	}
		Log.warning("SKAdmin exiting success="+ success);
		if (exitOnCompletion) {
			System.exit(success ? 0 : -1);
		}
    }
}

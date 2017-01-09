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
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.ClassVars;
import com.ms.silverking.cloud.dht.meta.ClassVarsZK;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.dht.meta.DaemonStateZK;
import com.ms.silverking.cloud.dht.meta.HealthMonitor;
import com.ms.silverking.cloud.dht.meta.InstanceExclusionZK;
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
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
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
	private final NamespaceOptions	skfsNSOptions; 
	private final NamespaceOptions	skfsMutableNSOptions;
	private final String	skGlobalCodebase;
	private final RingConfiguration		ringConfig;
	private final InstantiatedRingTree	ringTree;
	private final ExclusionSet			exclusionSet;
	
	private static final String	jreSuffix = "/jre";
	
	/*
	 * In the future, we should probably parallelize some metadata fetches that can be done in parallel in order
	 * to hide the latency of these operations. For now, we do not do this. 
	 */
	//private enum WorkType {GetHostGroups};
	
	//private static final String[]	_skfsNamespaces = {"fb.524288.b", "fb.131072.b", "fb.262144.b", "dht.health"};
	//private static final String[]	_skfsMutableNamespaces = {"dir", "attr"};
	private static final String[]	_skfsNamespaces = {};
	private static final String[]	_skfsMutableNamespaces = {"dir", "attr", "fb.524288.b", "fb.131072.b", "fb.262144.b", "dht.health"};
	private static final Set<String>	skfsNamespaces = ImmutableSet.copyOf(_skfsNamespaces);
	private static final Set<String>	skfsMutableNamespaces = ImmutableSet.copyOf(_skfsMutableNamespaces);
	
	private static final int	unsafeWarningCountdown = 20;
	
	private static final String	logFileName = "SKAdmin.out";
	
	public SKAdmin(SKGridConfiguration gc, SKAdminOptions options) throws IOException, KeeperException {
		Pair<RingConfiguration,InstantiatedRingTree>	ringConfigAndTree;
		
		this.gc = gc;
		this.options = options;
		dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gc);
		dhtConfig = dhtMC.getDHTConfiguration();
		
		ringConfigAndTree = getRing(dhtConfig, dhtMC);		
		if (ringConfigAndTree == null) {
			Log.warning("No current ring for this instance");
			ringConfig = null;
			ringTree = null;
			cloudMC = null;
			exclusionSet = null;
		} else {
			ringConfig = ringConfigAndTree.getV1();
			ringTree = ringConfigAndTree.getV2();
			cloudMC = new com.ms.silverking.cloud.meta.MetaClient(ringConfig.getCloudConfiguration(), dhtMC.getZooKeeper().getZKConfig());
			exclusionSet = new ExclusionZK(cloudMC).readLatestFromZK();
		}
		
		
		classVarsZK = new ClassVarsZK(dhtMC);
		if (dhtConfig.getDefaultClassVars() != null) {
			defaultClassVars = DHTConstants.defaultDefaultClassVars.overrideWith(classVarsZK.getClassVars(dhtConfig.getDefaultClassVars()));
		} else {
			defaultClassVars = DHTConstants.defaultDefaultClassVars;
		}
		// FUTURE - allow for a real retention interval
		// FUTURE - eliminate parse
		skfsNSOptions = NamespaceOptions.parse("versionMode=SINGLE_VERSION,storageType=FILE,consistencyProtocol="+ ConsistencyProtocol.TWO_PHASE_COMMIT +",defaultPutOptions={compression="+ options.compression +",checksumType=MURMUR3_32,checksumCompressedValues=false,version=0,},valueRetentionPolicy=<InvalidatedRetentionPolicy>{invalidatedRetentionIntervalSeconds=10},defaultGetOptions={nonExistenceResponse=NULL_VALUE}");
		skfsMutableNSOptions = NamespaceOptions.parse("versionMode=SYSTEM_TIME_NANOS,revisionMode=NO_REVISIONS,storageType=FILE,consistencyProtocol="+ ConsistencyProtocol.TWO_PHASE_COMMIT +",defaultPutOptions={compression="+ options.compression +",checksumType=MURMUR3_32,checksumCompressedValues=false,version=0,},valueRetentionPolicy=<InvalidatedRetentionPolicy>{invalidatedRetentionIntervalSeconds=10},defaultGetOptions={nonExistenceResponse=NULL_VALUE}");
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
				+DHTConstants.getSKInstanceLogDir(classVars, gc) +"/"+ DHTConstants.heapDumpFile;
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
	
	private String getDHTOptions(SKAdminOptions options, ClassVars classVars) {
		return "-Dcom.ms.silverking.Log="+ options.logLevel
				+" -D"+ DHTConstants.dataBasePathProperty +"="+ getDataDir(classVars)
				+" -D"+ DHTConstants.reapIntervalProperty +"="+ getReapInterval(classVars);
	}
	
	// FUTURE - make os specific commands generic
	private String createStartCommand(DHTConfiguration dhtConfig, ClassVars classVars, SKAdminOptions options) {
		String	daemonLogDir;
		String	daemonLogFile;
		String	prevDaemonLogFile;
		
		daemonLogDir = DHTConstants.getSKInstanceLogDir(classVars, gc);
		daemonLogFile = daemonLogDir +"/"+ DHTConstants.daemonLogFile;
		prevDaemonLogFile = daemonLogDir +"/"+ DHTConstants.prevDaemonLogFile;
		return 
				createStopCommand(dhtConfig, classVars) +"; "
				+"mkdir -p "+ getDataDir(classVars) +"; "
				+"mkdir -p "+ daemonLogDir +"; "
				+"mv "+ daemonLogFile +" "+ prevDaemonLogFile +"; "
				+ getPreJavaCommand(classVars) +" "
				+ getJavaCmdStart(options, classVars) 
				+" "+ DHTNode.class.getCanonicalName()
				+" -n "+ gc.getClientDHTConfiguration().getName() 
				+" -z "+ new ZooKeeperConfig(gc.getClientDHTConfiguration().getZkLocs()).toString()
				+" -into "+ options.inactiveNodeTimeoutSeconds
				+" 1>"+ daemonLogFile +" 2>&1 &"
				;
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
	 * Previous heap definitions allowed unitless quantities to be treated as MB. 
	 * Do that here for smaller unitless quantities
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
				+"export "+ GridConfiguration.defaultBaseEnvVar +"="+ GridConfiguration.defaultBase +"; " 
				+"export "+ DHTConstants.jaceHomeEnv +"="+ PropertiesHelper.envHelper.getString(DHTConstants.jaceHomeEnv, UndefinedAction.ExceptionOnUndefined) +"; "
				+"export "+ DHTConstants.javaHomeEnv +"="+ PropertiesHelper.envHelper.getString(DHTConstants.javaHomeEnv, getSystemJavaHome()) +"; "
				+"export "+ DHTConstants.classpathEnv +"="+ PropertiesHelper.envHelper.getString(DHTConstants.classpathEnv, System.getProperty(DHTConstants.classpathProperty)) +"; "
				+classVars.getVarMap().get(DHTConstants.checkSKFSCommandVar)
					+" -g "+ gc.getName() +" -z "+ new ZooKeeperConfig(gc.getClientDHTConfiguration().getZkLocs())
					+" -c "+ command +" -f "+ options.forceSKFSDirectoryCreation;
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
			result = result && _result;
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
		InstanceExclusionZK	instanceExclusionZK;
		
		instanceExclusionZK = new InstanceExclusionZK(dhtMC);
		instanceExclusionZK.writeToZK(options.newHost != null ? ExclusionSet.parse(options.newHost) : ExclusionSet.emptyExclusionSet(VersionedDefinition.NO_VERSION));
		return true;
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
		
		preferredServer = findArbitraryActiveServer(dhtConfig.getHostGroups(), hostGroupTable);
		Log.warning("Using preferredServer ", preferredServer);
		nsCreator = new SKFSNamespaceCreator(gc.getClientDHTConfiguration(), preferredServer);
		nsCreator.createNamespaces(skfsNamespaces, skfsNSOptions);
		nsCreator.createNamespaces(skfsMutableNamespaces, skfsMutableNSOptions);
		return true;
	}
	
	private Set<String> retainOnlySpecifiedAndNonExcludedServers(Set<String> servers) {
		Set<String>	_servers;
		
		_servers = new HashSet<>(servers);
		if (!options.includeExcludedHosts) {
			_servers.removeAll(exclusionSet.getServers());
		}
		if (options.newHost != null) {
			Set<String>	newHosts;
			Set<String>	_s;
			
			newHosts = CollectionUtil.parseSet(options.newHost, ",");
			//Log.warningf("options.newHost %s newHosts %d", options.newHost, newHosts.size());
			_s = new HashSet<>(_servers);
			_s.retainAll(newHosts);
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
					throw new RuntimeException("Fatal: Attempted to start ineligible servers: "+ CollectionUtil.toString(ineligibleServers, ','));
				} else {
					Log.countdownWarning("*** Including unsafe excluded servers. This may result in data loss ***", unsafeWarningCountdown);
				}
			} else {
				Log.warning("Server eligibility verified");
			}
		}		
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
		validActiveServers = retainOnlySpecifiedAndNonExcludedServers(validActiveServers);
		verifyServerEligibility(validActiveServers, commands);
		Log.warning("validActiveServers: ", CollectionUtil.toString(validActiveServers));
		
		passiveNodeHostGroupNames = dhtConfig.getPassiveNodeHostGroupsAsSet();
		Log.warning("passiveNodeHostGroupNames: ", CollectionUtil.toString(passiveNodeHostGroupNames));
		
		validPassiveServers = findValidPassiveServers(passiveNodeHostGroupNames, hostGroupTable);
		validPassiveServers = retainOnlySpecifiedAndNonExcludedServers(validPassiveServers);
		Log.warning("validPassiveServers: ", CollectionUtil.toString(validPassiveServers));
		
		result = true;
		for (SKAdminCommand command : commands) {
			boolean	_result;
			
			Log.warning("Executing cluster command: ", command);
			serverCommands = createServerCommands(command, validActiveServers, validPassiveServers, 
												hostGroupTable, hostGroupToClassVars, 
												activeHostGroupNames, passiveNodeHostGroupNames);
			displayCommandMap(serverCommands);
			if (!options.displayOnly) {
				_result = execCommandMap(serverCommands, validActiveServers.size() > 0 ? validActiveServers : validPassiveServers);
				result = result && _result;
				if (!result) {
					break;
				}
			}
			Log.warning("Waiting for nodes to enter running state...");
			if (command.equals(SKAdminCommand.StartNodes)) {
				waitUntilRunning(IPAndPort.set(validActiveServers, dhtConfig.getPort()));
			}
		}
		
		return result;
	}
	
	private boolean execCommandMap(Map<String, String[]> serverCommands, Set<String> workerCandidateHosts) throws IOException {
		TwoLevelParallelSSHMaster	sshMaster;
		boolean	result;
		
		sshMaster = new TwoLevelParallelSSHMaster(serverCommands, ImmutableList.copyOf(workerCandidateHosts), options.numWorkerThreads, options.timeoutSeconds, options.maxAttempts, false);
		Log.warning("Starting workers");
		sshMaster.startWorkers();
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
		allServers.addAll(validActiveServers);
		allServers.addAll(validPassiveServers);
		
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
				case ClearData:
					rawServerCommand = createClearDataCommand(dhtConfig, serverClassVars);
					break;
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
		activeAndPassiveHostGroupNames.addAll(acceptableHostGroups);
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
			return null;
		}
		serverClassVars = hostGroupToClassVars.get(serverHostGroup);
		if (serverClassVars != null) {
			return defaultClassVars.overrideWith(serverClassVars);
		} else {
			Log.warning(server +" has no ClassVars for group "+ serverHostGroup);
			return null;
		}
	}
	
	///////////////////////////////////////////
	
	public void waitUntilRunning(Set<IPAndPort> activeNodes) {
		DaemonStateZK	daemonStateZK;
		
        daemonStateZK = new DaemonStateZK(dhtMC);
		daemonStateZK.waitForQuorumState(activeNodes, DaemonState.RUNNING, Integer.MAX_VALUE);
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
	
	///////////////////////////////////////////
	
	public static void main(String[] args) {
    	try {
    		SKAdmin			skAdmin;
    		SKAdminOptions	options;
    		CmdLineParser	parser;
    		SKGridConfiguration	gc;
    		boolean			success;
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
    			return;
    		}
    		
    		fillDefaultOptions(options);
    		
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
    		Log.warning("SKAdmin exiting success="+ success);
    		//System.exit(success ? 0 : -1);
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}

package com.ms.silverking.cloud.dht.management;

import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.daemon.DHTNodeOptions;

class SKAdminOptions {
	static String	exclusionsTarget = "exclusions";
	static String	activeDaemonsTarget = "activeDaemons";
	
	static final int	skfsTimeoutNotSet = -1;
	
	SKAdminOptions() {
	}
	
	@Option(name="-g", usage="GridConfig", required=true)
	String	gridConfig;
	
	@Option(name="-G", usage="GridConfigBase", required=false)
	String	gridConfigBase;
	
	@Option(name="-c", usage="Command(s)", required=true)
	String	commands;
	
	@Option(name="-C", usage="Compression", required=false)
	Compression	compression = Compression.LZ4;
	
	@Option(name="-t", usage="target(s)", required=false)
	String	targets;
	
	boolean targetsEqualsExclusionsTarget() {
		return targets != null && targets.equalsIgnoreCase(exclusionsTarget);
	}
	
	boolean targetsEqualsActiveDaemonsTarget() {
		return targets != null && targets.equalsIgnoreCase(activeDaemonsTarget);
	}
	
	@Option(name="-e", usage="includeExcludedHosts", required=false)
	boolean	includeExcludedHosts;
	
	@Option(name="-L", usage="CoreLimit", required=false)
	String	coreLimit;
	
	@Option(name="-l", usage="LogLevel", required=false)
	String	logLevel = "WARNING";
	
	@Option(name="-cp", usage="ClassPath", required=false)
	String	classPath;
	
	@Option(name="-jb", usage="JavaBin", required=false)
	String	javaBinary;

	@Option(name="-ao", usage="AssertionOption", required=false)
	public String assertionOption = "-da";
	
	@Option(name="-po", usage="ProfilingOptions", required=false)
	public String profilingOptions = "";
	
	@Option(name="-wt", usage="NumWorkerThreads", required=false)
	public int numWorkerThreads = 6;
	
	@Option(name="-wto", usage="WorkerTimeoutSeconds", required=false)
	public int workerTimeoutSeconds = 5 * 60;
	
	@Option(name="-to", usage="TimeoutSeconds", required=false)
	public String timeoutSeconds = Integer.toString(3 * 60 * 60);
	
	@Option(name="-into", usage="InactiveNodeTimeoutSeconds", required=false)
	public int inactiveNodeTimeoutSeconds = DHTNodeOptions.defaultInactiveNodeTimeoutSeconds;
	
	@Option(name="-forceUnsafe", usage="forceInclusionOfUnsafeExcludedServers", required=false)
	boolean	forceInclusionOfUnsafeExcludedServers = false;
	
	@Option(name="-excludeInstanceExclusions", usage="excludeInstanceExclusions", required=false)
	boolean	excludeInstanceExclusions = false;
	
	@Option(name="-ma", usage="MaxAttempts", required=false)
	public int maxAttempts = 2;
	
	@Option(name="-D", usage="displayOnly", required=false)
	boolean	displayOnly;
	
	@Option(name="-fsdc", usage="forceSKFSDirectoryCreation", required=false)
	boolean	forceSKFSDirectoryCreation;
	
	@Option(name="-r", usage="disableReap", required=false)
	boolean disableReap = false;
	
	@Option(name="-destructive", usage="destructive", required=false)
	boolean	destructive = false;
	
	@Option(name="-leaveTrash", usage="leaveTrash", required=false)
	boolean leaveTrash = false;
	
	@Option(name="-opTimeoutController", usage="opTimeoutController", required=false)
	public String opTimeoutController = "<OpSizeBasedTimeoutController>{maxAttempts=5,constantTime_ms=300000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1200000}";

	@Option(name="-dirNSPutTimeoutController", usage="dirNSPutTimeoutController", required=false)
	public String dirNSPutTimeoutController = "<OpSizeBasedTimeoutController>{maxAttempts=12,constantTime_ms=60000,itemTime_ms=305,nonKeyedOpMaxRelTimeout_ms=1200000}";
	
	@Option(name="-fileBlockNSValueRetentionPolicy", usage="fileBlockNSValueRetentionPolicy", required=false)
	public String fileBlockNSValueRetentionPolicy;
	
	@Option(name="-defaultClassVars", usage="defaultClassVars", required=false)
	public String defaultClassVars;
	
	@Option(name="-ps", usage="PreferredServer", required=false)
	public String preferredServer;	
	
	@Option(name="-skfsEntryTimeoutSecs", usage="skfsEntryTimeoutSecs", required=false)
	public int	skfsEntryTimeoutSecs = skfsTimeoutNotSet;	
	
	@Option(name="-skfsAttrTimeoutSecs", usage="skfsAttrTimeoutSecs", required=false)
	public int	skfsAttrTimeoutSecs = skfsTimeoutNotSet;	
	
	@Option(name="-skfsNegativeTimeoutSecs", usage="skfsNegativeTimeoutSecs", required=false)
	public int	skfsNegativeTimeoutSecs = skfsTimeoutNotSet;
	
	@Option(name="-pinToNICLocalCPUs", usage="pinToNICLocalCPUs", required=false)
	public String pinToNICLocalCPUs;
}

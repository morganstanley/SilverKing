package com.ms.silverking.cloud.dht.management;

import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.daemon.DHTNodeOptions;

class SKAdminOptions {
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
	
	@Option(name="-H", usage="newHost(s)", required=false)
	String	newHost;
	
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
	
	@Option(name="-to", usage="TimeoutSeconds", required=false)
	public int timeoutSeconds = 5 * 60;
	
	@Option(name="-into", usage="InactiveNodeTimeoutSeconds", required=false)
	public int inactiveNodeTimeoutSeconds = DHTNodeOptions.defaultInactiveNodeTimeoutSeconds;
	
	@Option(name="-forceUnsafe", usage="forceInclusionOfUnsafeExcludedServers", required=false)
	boolean	forceInclusionOfUnsafeExcludedServers = false;
	
	@Option(name="-ma", usage="MaxAttempts", required=false)
	public int maxAttempts = 2;
	
	@Option(name="-D", usage="displayOnly", required=false)
	boolean	displayOnly;
	
	@Option(name="-fsdc", usage="forceSKFSDirectoryCreation", required=false)
	boolean	forceSKFSDirectoryCreation;
}

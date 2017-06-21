package com.ms.silverking.cloud.dht.meta;

import org.kohsuke.args4j.Option;

class StaticDHTCreatorOptions {
	StaticDHTCreatorOptions() {
	}
	
	static final int	defaultPort = 7575;
	
	@Option(name="-z", usage="zkEnsemble", required=true)
	String	zkEnsemble;
	
	@Option(name="-f", usage="serverFile", required=false)
	String	serverFile;
	
	@Option(name="-s", usage="servers", required=false)
	String	servers;
	
	@Option(name="-g", usage="gridConfig", required=false)
	String	gridConfig;
	
	@Option(name="-G", usage="gridConfigDir", required=true)
	String	gridConfigDir;
	
	@Option(name="-L", usage="skInstanceLogBaseVar", required=false)
	String	skInstanceLogBaseVar;
	
	@Option(name="-D", usage="dataBaseVar", required=false)
	String	dataBaseVar;
	
	@Option(name="-d", usage="dhtName", required=false)
	String	dhtName;
	
	@Option(name="-r", usage="replication", required=false)
	int	replication = 1;
	
	@Option(name="-p", usage="port", required=false)
	int	port = defaultPort;
	
	@Option(name="-i", usage="initialHeapSize", required=false)
	int	initialHeapSize = 1024;
	
	@Option(name="-x", usage="maxHeapSize", required=false)
	int	maxHeapSize = 4096;	
	
	@Option(name="-n", usage="nsCreationOptions", required=false)
	String	nsCreationOptions;
	
    @Option(name="-v", usage="verbose")
    boolean verbose;
    
	@Option(name="-k", usage="skfsConfigurationFile", required=false)
	String	skfsConfigurationFile;    
}
package com.ms.silverking.cloud.dht.management;

import java.io.File;

import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.daemon.storage.convergence.management.RingMasterControlImpl;

public class SKAdminShellOptions {
    @Option(name="-g", usage="GridConfig", required=true)
    public String gridConfig;
    
    @Option(name="-c", usage="commands")
    String  commands;
	
    @Option(name="-f", usage="commandFile")
    File    commandFile;    
    
	@Option(name="-s", usage="server")
	String	server = "localhost";
	
	@Option(name="-p", usage="port")
	int	port = RingMasterControlImpl.defaultRegistryPort;
}

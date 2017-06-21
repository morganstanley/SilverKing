package com.ms.silverking.cloud.zookeeper;

import org.kohsuke.args4j.Option;

public class LocalZKOptions {
	LocalZKOptions() {
	}
		
	@Option(name="-d", usage="dataDir", required=false)
	String	dataDir;
	
	@Option(name="-p", usage="port", required=false)
	int	port = 0;
}

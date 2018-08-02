package com.ms.silverking.cloud.dht.management;

import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.daemon.DHTNodeOptions;

class SKCloudAdminOptions {
	SKCloudAdminOptions() {
	}
	
//	command
	
	@Option(name="-n", usage="NumberOfInstances", required=true)
	int numInstances;
//	
//	@Option(name="-i", usage="instanceType", required=false)
//	String	gridConfigBase;
//	
//	@Option(name="-e", usage="excludeMaster", required=true)
//	String	commands;
//	
//	@Option(name="-a", usage="amiId", required=false)
//	Compression	compression = Compression.LZ4;
}

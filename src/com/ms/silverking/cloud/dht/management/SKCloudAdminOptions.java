package com.ms.silverking.cloud.dht.management;

import org.kohsuke.args4j.Option;

class SKCloudAdminOptions {
	
	static final int defaultNumInstances = -1;
	
	SKCloudAdminOptions() {
	}
	
	@Option(name="-c", usage="command. eg: \"launchInstances\", \"stopInstances\", or \"terminateInstances\"", required=true)
	String command;
	
	@Option(name="-n", usage="numberOfInstances. eg: \"1\", \"50\", \"1000\", etc.", required=false)
	int numInstances = defaultNumInstances;

	@Option(name="-a", usage="amiId. eg: \"68790210\", \"bfe4b5c7\", etc.", required=false)
	String amiId = null;
	
	@Option(name="-i", usage="instanceType. eg: \"t2.micro\", \"m5d.large\", \"i3.metal\", etc.", required=false)
	String instanceType = null;
	
	@Option(name="-e", usage="excludeMaster", required=false)
	boolean excludeMaster = false;
	
}

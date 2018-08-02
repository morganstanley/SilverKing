package com.ms.silverking.cloud.dht.management;

import org.kohsuke.args4j.Option;

class SKCloudAdminOptions {
	
	static final int defaultNumInstances = -1;
	
	SKCloudAdminOptions() {
	}
	
	@Option(name="-c", aliases={"--command"}, usage="command. eg: \"launchInstances\", \"stopInstances\", or \"terminateInstances\"", required=true)
	String command;
	
	@Option(name="-n", aliases={"--num-of-instances"}, usage="numberOfInstances. eg: \"1\", \"50\", \"1000\", etc.", required=false)
	int numInstances = defaultNumInstances;

	@Option(name="-a", aliases={"--ami-id"}, usage="amiId. eg: \"68790210\", \"bfe4b5c7\", etc.", required=false)
	String amiId = null;
	
	@Option(name="-i", aliases={"--instance-type"}, usage="instanceType. eg: \"t2.micro\", \"m5d.large\", \"i3.metal\", etc.", required=false)
	String instanceType = null;
	
	@Option(name="-e", aliases={"--exclude-master"}, usage="excludeMaster", required=false)
	boolean excludeMaster = false;
	
}

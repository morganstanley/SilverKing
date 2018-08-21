package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.cloud.dht.management.aws.Util.findStoppedInstancesWithKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getInstanceIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIps;
import static com.ms.silverking.cloud.dht.management.aws.Util.newKeyName;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.printNoDot;
import static com.ms.silverking.cloud.dht.management.aws.Util.waitForInstancesToBeReachable;
import static com.ms.silverking.cloud.dht.management.aws.Util.waitForInstancesToBeRunning;

import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;

public class MultiInstanceStarter {

	private final AmazonEC2 ec2;
	private final String keyPair;
	private List<Instance> instances;
	
	public MultiInstanceStarter(AmazonEC2 ec2, String keyPair) {
		this.ec2     = ec2;
		this.keyPair = keyPair;
		
		instances = null;
	}
	
	public void run() {
		instances = findStoppedInstancesWithKeyPair(ec2, keyPair);
		startInstances();
		waitForInstancesToBeRunning(  ec2, instances);
		waitForInstancesToBeReachable(ec2, instances);
	}
	
	private void startInstances() {
		printNoDot("Starting Instances");
		
		List<String> ips = getIps(instances);
		for (String ip : ips)
			System.out.println("    " + ip);
		StartInstancesRequest startInstancesRequest = new StartInstancesRequest();
		startInstancesRequest.withInstanceIds( getInstanceIds(instances) );
		
		StartInstancesResult result = ec2.startInstances(startInstancesRequest);
		List<InstanceStateChange> startingInstances = result.getStartingInstances();

		print("");
		printDone( getIds(startingInstances) );
	}
	
    public static void main(String[] args) {
        System.out.println("Attempting to start all instances with keypair: " + newKeyName);
        MultiInstanceStarter starter = new MultiInstanceStarter(AmazonEC2ClientBuilder.defaultClient(), newKeyName);
        starter.run();
	}

}

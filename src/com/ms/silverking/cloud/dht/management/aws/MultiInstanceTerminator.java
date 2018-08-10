package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.cloud.dht.management.aws.Util.deleteKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.findRunningInstancesWithKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getInstanceIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIps;
import static com.ms.silverking.cloud.dht.management.aws.Util.newKeyName;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.printNoDot;

import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

public class MultiInstanceTerminator {

	private final AmazonEC2 ec2;
	private final String keyPair;
	private List<Instance> instances;
	
	public MultiInstanceTerminator(AmazonEC2 ec2, String keyPair) {
		this.ec2     = ec2;
		this.keyPair = keyPair;
		
		instances = null;
	}
	
	public void run() {
		instances = findRunningInstancesWithKeyPair(ec2, keyPair);
		terminateInstances();
		deleteKeyPair(ec2);
	}
	
	private void terminateInstances() {
		printNoDot("Terminating Instances");

		if (instances.isEmpty())
			return;
		
		List<String> ips = getIps(instances);
		for (String ip : ips)
			System.out.println("    " + ip);
		TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();
		terminateInstancesRequest.withInstanceIds( getInstanceIds(instances) );
		
		TerminateInstancesResult result = ec2.terminateInstances(terminateInstancesRequest);
		List<InstanceStateChange> terminatingInstances = result.getTerminatingInstances();

		print("");
		printDone( String.join(", ", getIds(terminatingInstances)) );
	}
	
    public static void main(String[] args) {
        System.out.println("Attempting to terminate all instances with keypair: " + newKeyName);
        MultiInstanceTerminator terminator = new MultiInstanceTerminator(AmazonEC2ClientBuilder.defaultClient(), newKeyName);
        terminator.run();
	}

}

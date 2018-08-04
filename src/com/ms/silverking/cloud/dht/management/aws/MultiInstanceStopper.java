package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.cloud.dht.management.aws.Util.ec2Client;
import static com.ms.silverking.cloud.dht.management.aws.Util.findInstancesRunningWithKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getInstanceIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIps;
import static com.ms.silverking.cloud.dht.management.aws.Util.newKeyName;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.printNoDot;

import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;

public class MultiInstanceStopper {

	private final AmazonEC2 ec2;

	private List<Instance> instances;
	
	private List<InstanceStateChange> workerInstances;
	
	public MultiInstanceStopper(AmazonEC2 ec2) {
		this.ec2 = ec2;
		
		instances = null;
	}
	
	public void run() {
		instances = findInstancesRunningWithKeyPair(ec2);
		stopInstances();
	}
	
	private void stopInstances() {
		printNoDot("Stopping Instances");
		
		List<String> ips = getIps(instances);
		for (String ip : ips)
			System.out.println("    " + ip);
		StopInstancesRequest stopInstancesRequest = new StopInstancesRequest();
		stopInstancesRequest.withInstanceIds( getInstanceIds(instances) );
		
		StopInstancesResult result = ec2.stopInstances(stopInstancesRequest);
		workerInstances = result.getStoppingInstances();

		print("");
		printDone( String.join(", ", getIds(workerInstances)) );
	}
	
    public static void main(String[] args) {
        System.out.println("Attempting to stop all instances with keypair: " + newKeyName);
        MultiInstanceStopper stopper = new MultiInstanceStopper(ec2Client);
        stopper.run();
	}

}

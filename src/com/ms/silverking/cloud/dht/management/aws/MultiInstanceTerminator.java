package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.cloud.dht.management.aws.Util.debugPrint;
import static com.ms.silverking.cloud.dht.management.aws.Util.deleteKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.getIps;
import static com.ms.silverking.cloud.dht.management.aws.Util.getInstanceIds;
import static com.ms.silverking.cloud.dht.management.aws.Util.isKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.isRunning;
import static com.ms.silverking.cloud.dht.management.aws.Util.newKeyName;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.printInstance;
import static com.ms.silverking.cloud.dht.management.aws.Util.printNoDot;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

public class MultiInstanceTerminator {

	private final AmazonEC2 ec2;

	private List<Instance> instances;
	
	private List<InstanceStateChange> workerInstances;
	
	public MultiInstanceTerminator(AmazonEC2 ec2) {
		this.ec2 = ec2;
		
		instances = new ArrayList<>();
	}
	
	public void run() {
		findInstancesRunningWithKeyPair();
		terminateInstances();
		deleteKeyPair(ec2);
	}
	
	private void findInstancesRunningWithKeyPair() {
		print("Finding Running Instances");
		
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		while (true) {
		    DescribeInstancesResult response = ec2.describeInstances(request);

		    for (Reservation reservation : response.getReservations()) {
		        for (Instance instance : reservation.getInstances()) {
		            printInstance(instance);
			        if ( isRunning(instance) && isKeyPair(instance) ) {
			        	instances.add(instance);
			        }
		        }
		    }

		    if (response.getNextToken() == null)
		        break;
		    
		    debugPrint("token: " + response.getNextToken());
		    request.setNextToken(response.getNextToken());
		}

		printDone("found " + instances.size());
	}
	
	private void terminateInstances() {
		printNoDot("Terminating Instances");
		
		List<String> ips = getIps(instances);
		for (String ip : ips)
			System.out.println("    " + ip);
		TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest();
		terminateInstancesRequest.withInstanceIds( getInstanceIds(instances) );
		
		TerminateInstancesResult result = ec2.terminateInstances(terminateInstancesRequest);
		workerInstances = result.getTerminatingInstances();

		print("");
		printDone( String.join(", ", getIds(workerInstances)) );
	}
	
	private List<String> getIds(List<InstanceStateChange> instanceStateChanges) {
		List<String> ids = new ArrayList<>();
		
		for (InstanceStateChange instanceStateChange : instanceStateChanges)
			ids.add(instanceStateChange.getInstanceId());
		
		return ids;
	}
	
    public static void main(String[] args) {
        System.out.println("Attempting to terminate all instances with keypair: " + newKeyName);
        MultiInstanceTerminator terminator = new MultiInstanceTerminator(AmazonEC2ClientBuilder.defaultClient());
        terminator.run();
	}

}

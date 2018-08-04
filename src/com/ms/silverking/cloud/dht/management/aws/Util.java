package com.ms.silverking.cloud.dht.management.aws;

import static com.ms.silverking.cloud.dht.management.aws.Util.debugPrint;
import static com.ms.silverking.cloud.dht.management.aws.Util.isKeyPair;
import static com.ms.silverking.cloud.dht.management.aws.Util.isRunning;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.printInstance;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DeleteKeyPairRequest;
import com.amazonaws.services.ec2.model.DeleteKeyPairResult;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateChange;
import com.amazonaws.services.ec2.model.Reservation;

public class Util {

	public static final AmazonEC2 ec2Client = AmazonEC2ClientBuilder.defaultClient();
	
	public static final String userHome = System.getProperty("user.home");
	       static final String newLine  = System.getProperty("line.separator");
	
	static boolean debugPrint = false;
	
	static final String newKeyName = "sk_key";
	
	static void printInstance(Instance instance) {
		if (debugPrint)
			System.out.printf(
	                "Found instance with id %s, " +
	                "AMI %s, " +
	                "type %s, " +
	                "state %s " +
	                "and monitoring state %s%n",
	                instance.getInstanceId(),
	                instance.getImageId(),
	                instance.getInstanceType(),
	                instance.getState().getName(),
	                instance.getMonitoring().getState());
	}
	
	static void printNoDot(String text) {
		printHelper(text, "");
		System.out.println();
	}
	
	public static void print(String text) {
		printHelper(text, "...");
	}
	
	static void printHelper(String text, String spacer) {
		System.out.printf("%-39s %-3s ", text, spacer);
	}
	
	public static void printDone(String value) {
		System.out.println("done ("+value+")");
	}
	
	static void debugPrint(String text) {
		if (debugPrint)
			System.out.println(text);
	}
	
	static List<String> getInstanceIds(List<Instance> instances) {
		List<String> ids = new ArrayList<>();
		
		for (Instance instance : instances)
			ids.add(instance.getInstanceId());
		
		return ids;
	}
	
	static List<String> getIps(List<Instance> instances) {
		List<String> ips = new ArrayList<>();
		
		for (Instance instance : instances)
			ips.add(instance.getPrivateIpAddress());
		
		return ips;
	}
	
	static List<Instance> findInstancesRunningWithKeyPair(AmazonEC2 ec2) {
		print("Finding Running Instances");

		List<Instance> instances = new ArrayList<>();
		
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
		
		return instances;
	}

	static List<String> getIds(List<InstanceStateChange> instanceStateChanges) {
		List<String> ids = new ArrayList<>();
		
		for (InstanceStateChange instanceStateChange : instanceStateChanges)
			ids.add(instanceStateChange.getInstanceId());
		
		return ids;
	}
		
	static boolean isRunning(Instance instance) {
		return instance.getState().getName().equals("running");
	}
	
	static boolean isKeyPair(Instance instance) {
		return instance.getKeyName().equals(newKeyName);
	}
	
	static void deleteKeyPair(AmazonEC2 ec2) {
		print("Deleting Old Key Pair");
		
		DeleteKeyPairRequest deleteKeyPairRequest = new DeleteKeyPairRequest();
		deleteKeyPairRequest.withKeyName(newKeyName);

		DeleteKeyPairResult deleteKeyPairResult = ec2.deleteKeyPair(deleteKeyPairRequest);
		
		printDone(newKeyName);
	}
	
	public static void checkNumInstances(int numInstances) {
		if (numInstances <= 0)
			throwIllegalArgumentException("numInstances", numInstances, "must be > 0");
	}

	public static void throwIllegalArgumentException(String variableName, Object variableValue, String msg) {
		throw new IllegalArgumentException("Invalid " + variableName + ": \"" + variableValue + "\" .... " + msg);
	}
}

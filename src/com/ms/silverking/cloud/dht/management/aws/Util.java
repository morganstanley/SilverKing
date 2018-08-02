package com.ms.silverking.cloud.dht.management.aws;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DeleteKeyPairRequest;
import com.amazonaws.services.ec2.model.DeleteKeyPairResult;
import com.amazonaws.services.ec2.model.Instance;

public class Util {

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
	
	static void print(String text) {
		printHelper(text, "...");
	}
	
	static void printHelper(String text, String spacer) {
		System.out.printf("%-39s %-3s ", text, spacer);
	}
	
	static void printDone(String value) {
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
}

package com.ms.silverking.cloud.dht.management;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.ms.silverking.cloud.dht.management.aws.MultiInstanceLauncher;

/**
 * <p>Tool responsible for executing most administrative SilverKing cloud commands.
 * E.g. used to stop and start SilverKing cloud instances.</p>
 * 
 * <p>Shell scripts used to launch this and other administrative commands should
 * contain minimal logic. Any "real work" should be done here (and in the other
 * administrative tools' Java implementations.)</p>
 */
public class SKCloudAdmin {
	
	private int numInstances;
	
//	public SKCloudAdmin(String amiId, String instanceType) {
//		Preconditions.checkNotNull(amiId);
//		Preconditions.checkNotNull(instanceType);
//	}
	public SKCloudAdmin(int numInstances) {
		checkNumInstances(numInstances);
		
		this.numInstances = numInstances;
	}
	
	void checkNumInstances(int numInstances) {
		if (numInstances <= 0)
			throw new IllegalArgumentException("numInstances must be > 0");
	}
	
	public void run() {
		try {
			MultiInstanceLauncher launcher = new MultiInstanceLauncher(InetAddress.getLocalHost(), numInstances, AmazonEC2ClientBuilder.defaultClient());
	        launcher.run();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
    	try {
    		SKCloudAdminOptions options = new SKCloudAdminOptions();
    		CmdLineParser parser = new CmdLineParser(options);
    		try {
    			parser.parseArgument(args);
    		} catch (CmdLineException exception) {
    			System.err.println(exception.getMessage());
    			parser.printUsage(System.err);
                System.exit(-1);
    		}
    		
    		SKCloudAdmin cloudAdmin = new SKCloudAdmin(options.numInstances);
    		cloudAdmin.run();
    	} catch (Exception e) {
    		e.printStackTrace();
            System.exit(-1);
    	}
	}
}
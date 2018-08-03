package com.ms.silverking.cloud.dht.management;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.ms.silverking.cloud.dht.management.aws.MultiInstanceLauncher;
import com.ms.silverking.cloud.dht.management.aws.Util;

/**
 * <p>Tool responsible for executing most administrative SilverKing cloud commands.
 * E.g. used to stop and start SilverKing cloud instances.</p>
 * 
 * <p>Shell scripts used to launch this and other administrative commands should
 * contain minimal logic. Any "real work" should be done here (and in the other
 * administrative tools' Java implementations.)</p>
 */
public class SKCloudAdmin {

	static final String launchInstancesCommand    = getCommandName("launch");
	static final String stopInstancesCommand      = getCommandName("stop");
	static final String terminateInstancesCommand = getCommandName("terminate");
	private static final List<String> commands = Arrays.asList(launchInstancesCommand, stopInstancesCommand, terminateInstancesCommand);
	
	private String  command;
	private int     numInstances;
	private String  amiId;
	private String  instanceType;
	private boolean includeMaster;
	
	private static String getCommandName(String command) {
		return command+"Instances";
	}
	
	// convenience ctor for stop/terminate testing
	SKCloudAdmin(String command) {
		this(command, SKCloudAdminOptions.defaultNumInstances, null, null, true);
	}
	
	public SKCloudAdmin(String command, int numInstances, String amiId, String instanceType, boolean includeMaster) {
		checkCommand(command);
		checkNumInstances(command, numInstances);
		
		this.command       = command;
		this.numInstances  = numInstances;
		this.amiId         = amiId;
		this.instanceType  = instanceType;
		this.includeMaster = includeMaster;
	}
	
	void checkCommand(String command) {
		if (!commands.contains(command))
			Util.throwIllegalArgumentException("command", command, "must be: " + commands);
	}
	
	void checkNumInstances(String command, int numInstances) {
		if (notALaunchCommand(command))
			return;
		
		Util.checkNumInstances(numInstances);
	}
	
	private boolean notALaunchCommand(String command) {
		return !command.equals(launchInstancesCommand);
	}
	
	public String getCommand() {
		return command;
	}
	
	public int getNumInstances() {
		return numInstances;
	}
	
	public String getAmiId() {
		return amiId;
	}
	
	public String getInstanceType() {
		return instanceType;
	}
	
	public boolean getIncludeMaster() {
		return includeMaster;
	}
	
	public void run() {
		switch (command) {
	        case "launchInstances":
	        	launchInstances();
	            break;
	        case "stopInstances":
	            stopInstances();
	            break;
	        case "terminateInstances":
	            terminateInstances();
	            break;
	        default: 
	            throw new RuntimeException("It shouldn't have been possible to reach here, but somehow we got here with this unknown command: " + command);
		}
	}
	
	private void launchInstances() {
		try {
			MultiInstanceLauncher launcher = new MultiInstanceLauncher(InetAddress.getLocalHost().getHostAddress(), AmazonEC2ClientBuilder.defaultClient(), numInstances, amiId, instanceType, includeMaster);
			launcher.run();
			List<String> instanceIps = launcher.getIpList();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	private void stopInstances() {
		System.out.println("Stopping");
	}
	
	private void terminateInstances() {
		System.out.println("Terminating");
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
    		
    		SKCloudAdmin cloudAdmin = new SKCloudAdmin(options.command, options.numInstances, options.amiId, options.instanceType, !options.excludeMaster);
    		cloudAdmin.run();
    	} catch (Exception e) {
    		e.printStackTrace();
            System.exit(-1);
    	}
	}
}
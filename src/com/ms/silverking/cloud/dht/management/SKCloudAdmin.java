package com.ms.silverking.cloud.dht.management;

import static com.ms.silverking.cloud.dht.management.aws.MultiInstanceLauncher.privateKeyFilename;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.userHome;

import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.ms.silverking.cloud.dht.management.aws.MultiInstanceLauncher;
import com.ms.silverking.cloud.dht.management.aws.Util;
import com.ms.silverking.cloud.dht.meta.StaticDHTCreator;

/**
 * <p>Tool responsible for executing most administrative SilverKing cloud commands.
 * E.g. used to stop and start SilverKing cloud instances.</p>
 * 
 * <p>Shell scripts used to launch this and other administrative commands should
 * contain minimal logic. Any "real work" should be done here (and in the other
 * administrative tools' Java implementations.)</p>
 */
public class SKCloudAdmin {

	public  static final String cloudOutDir = userHome + "/SilverKing/bin/cloud_out/";
	private static final String cloudGcName = "GC_SK_cloud";
	
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
		String launchHost = "";
		try {
			launchHost = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		MultiInstanceLauncher launcher = new MultiInstanceLauncher(launchHost, AmazonEC2ClientBuilder.defaultClient(), numInstances, amiId, instanceType, includeMaster);
		launcher.run();
		// from the launch/master host's perspective (i.e. where we are running this script from):
		//   1. this machine has the private key (created from MultiInstanceLauncher)
		//      we need to create it's corresponding public key so we can ssh to this instance (i.e. ourselves - localhost)
		//   2. each of the instances created from MultiInstanceLauncher already have the public key
		//      we need to put the private key on all those machines
		generatePublicKeyAndAddToAuthorizedKeys();
		List<String> workerIps = launcher.getWorkerIps();
		copyPrivateKeyToWorkerMachines(workerIps);
		startZk();
		runStaticInstanceCreator(launchHost, launcher.getInstanceIps());
		copyGcToWorkerMachines(workerIps);
		symlinkSkfsdOnWorkerMachines(workerIps);
	}
	
	private void generatePublicKeyAndAddToAuthorizedKeys() {
		print("Generating Pub Key and Adding to Auth Keys");

		runCommand("ssh-keygen -y -f " + privateKeyFilename + " >> " + userHome + "/.ssh/authorized_keys");
		
		printDone("");
	}
	
	private void runCommand(String command) {
		try {
	        java.lang.Runtime rt = java.lang.Runtime.getRuntime();
	        java.lang.Process p = rt.exec(command);
	        p.waitFor();
	        System.out.println("Process exited with code = " + p.exitValue());
	        // Get process' output: its InputStream
	        java.io.InputStream inputStream = p.getInputStream();
	        java.io.BufferedReader reader = new java.io.BufferedReader(new InputStreamReader(inputStream));
	        // And print each line
	        String line = null;
	        while ((line = reader.readLine()) != null) {
	            System.out.println(line);
	        }
	        inputStream.close();
		}
		catch (Exception e) {
			
		}
	}
	
	private void copyPrivateKeyToWorkerMachines(List<String> workerIps) {
		print("Copying private key to workers");

		for (String workerIp : workerIps)
			scpFile(userHome + "/.ssh", privateKeyFilename, workerIp);
		
		printDone("");
	}
	
	private void scpFile(String destDir, String file, String host) {
		String user = System.getProperty("user.name");
		runCommand("scp -o StrictHostKeyChecking=no " + file + " " + user + "@" + host + ":" + destDir);
	}
	
	private void startZk() {
		print("Starting ZooKeeper");

		runCommand(userHome + "/SilverKing/build/aws/zk_start.sh");
		
		printDone("");
	}
	
	private void runStaticInstanceCreator(String launchHost, List<String> instanceIps) {
		StaticDHTCreator.main(new String[]{"-G", cloudOutDir, "-g", cloudGcName, "-d", "SK_cloud", 
										   "-s", String.join(",", instanceIps), "-r", "1", "-z", launchHost+":2181",
										   "-D", "/var/tmp/silverking", "-L", "/tmp/silverking", "-k", cloudOutDir+"/../skfs", "-i", "10"/*in M's*/});
	}
	
	private void copyGcToWorkerMachines(List<String> workerIps) {
		print("Copying GC to workers");

		String srcDir = cloudOutDir;
		for (String workerIp : workerIps)
			scpFile(srcDir, srcDir+"/"+cloudGcName+".env", workerIp);
		
		printDone("");
	}
	
	private void symlinkSkfsdOnWorkerMachines(List<String> workerIps) {
		print("Symlinking skfsd on workers");

		for (String workerIp : workerIps)
			ssh(workerIp, "ln -sv " + cloudOutDir+"/../build/skfs-build/skfs-install/arch-output-area/skfsd" + " " + cloudOutDir+"/../skfs/skfsd");
		
		printDone("");
	}
	
	private void ssh(String host, String command) {
		runCommand("ssh -x -o StrictHostKeyChecking=no " + host + " " + command);
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
package com.ms.silverking.cloud.dht.management;

import static com.ms.silverking.cloud.dht.management.aws.MultiInstanceLauncher.privateKeyFilename;
import static com.ms.silverking.cloud.dht.management.aws.Util.newKeyName;
import static com.ms.silverking.cloud.dht.management.aws.Util.print;
import static com.ms.silverking.cloud.dht.management.aws.Util.printDone;
import static com.ms.silverking.cloud.dht.management.aws.Util.userHome;
import static com.ms.silverking.process.ProcessExecutor.runBashCmd;
import static com.ms.silverking.process.ProcessExecutor.runCmd;
import static com.ms.silverking.process.ProcessExecutor.scpFile;
import static com.ms.silverking.process.ProcessExecutor.ssh;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.ms.silverking.cloud.dht.management.aws.MultiInstanceLauncher;
import com.ms.silverking.cloud.dht.management.aws.MultiInstanceStarter;
import com.ms.silverking.cloud.dht.management.aws.MultiInstanceStopper;
import com.ms.silverking.cloud.dht.management.aws.MultiInstanceTerminator;
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

	public  static final String cloudOutDir = userHome + "/SilverKing/bin/cloud_out";
	private static final String cloudGcName = "GC_SK_cloud";

	static final String launchInstancesCommand    = getCommandName("launch");
	static final String startInstancesCommand     = getCommandName("start");
	static final String stopInstancesCommand      = getCommandName("stop");
	static final String terminateInstancesCommand = getCommandName("terminate");
	private static final List<String> commands = Arrays.asList(launchInstancesCommand, startInstancesCommand, stopInstancesCommand, terminateInstancesCommand);
	
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
	        case "startInstances":
	            startInstances();
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
		System.out.println("=== LAUNCHING INSTANCES ===");
		String launchHost = "";
		try {
			launchHost = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		MultiInstanceLauncher launcher = new MultiInstanceLauncher(AmazonEC2ClientBuilder.defaultClient(), launchHost, numInstances, amiId, instanceType, includeMaster);
		launcher.run();
		// from the launch/master host's perspective (i.e. where we are running this script from):
		//   1. this machine has the private key (created from MultiInstanceLauncher)
		//      we need to create it's corresponding public key AND add it to authorized_keys so we can ssh to this instance (i.e. ourselves - localhost)
		//      we don't need the id_rsa.pub to hang around, we just need the CONTENTS of it in authorized_keys
		//   2. each of the instances created from MultiInstanceLauncher already have the public key (they actually don't have the public key, i.e. there's 
		//      no .ssh/id_rsa.pub on those machines, but the content of the public key is in authorized_keys, and that's all that there needs to be for ssh'ing)
		//      we need to put the private key on all those machines
		generatePublicKeyAndAddToAuthorizedKeys();
		List<String> workerIps = launcher.getWorkerIps();
		if (!launcher.isMasterOnlyInstance())
			copyPrivateKeyToWorkerMachines(workerIps);
		startZk();
		List<String> masterAndWorkerIps = launcher.getInstanceIps();
		runStaticInstanceCreator(launchHost, masterAndWorkerIps);
		if (!launcher.isMasterOnlyInstance())
			copyGcToWorkerMachines(workerIps);
		symlinkSkfsdOnAllMachines(masterAndWorkerIps);
		printNextSteps();
	}
	
	private void generatePublicKeyAndAddToAuthorizedKeys() {
		print("Generating public key and adding it to authorized_keys");

		// using runBashCmd instead of runCmd, b/c of chained command ">>"
		runBashCmd("ssh-keygen -y -f " + privateKeyFilename + " >> " + userHome + "/.ssh/authorized_keys");
		
		printDone("");
	}
	
	private void copyPrivateKeyToWorkerMachines(List<String> workerIps) {
		print("Copying private key to workers");

		for (String workerIp : workerIps)
			scpFile(userHome + "/.ssh", privateKeyFilename, workerIp);
		
		printDone("");
	}
	
	private void startZk() {
		print("Starting ZooKeeper");

		runCmd(userHome + "/SilverKing/build/aws/zk_start.sh");
		
		printDone("");
	}
	
	private void runStaticInstanceCreator(String launchHost, List<String> instanceIps) {
		print("Running Static Instance Creator");

		StaticDHTCreator.main(new String[]{"-G", cloudOutDir, "-g", cloudGcName, "-d", "SK_cloud", 
										   "-s", String.join(",", instanceIps), "-r", "1", "-z", launchHost+":2181",
										   "-D", "/var/tmp/silverking", "-L", "/tmp/silverking", "-k", cloudOutDir+"/../lib/skfs.config", "-i", "10"/*in M's*/});
		
		printDone("");
	}
	
	private void copyGcToWorkerMachines(List<String> workerIps) {
		print("Copying GC to workers");

		String srcDir = cloudOutDir;
		for (String workerIp : workerIps) {
			ssh(workerIp, "mkdir -p " + srcDir);
			scpFile(srcDir, srcDir+"/"+cloudGcName+".env", workerIp);
		}
		
		printDone("");
	}
	
	// symlink even this launch host (master) b/c if it's included in the instances, we need skfsd on here, which it isn't
	// and if it isn't a part of the instances, then it's just a harmless symlink 
	// that's why we're using instanceIps instead of just workerIps
	private void symlinkSkfsdOnAllMachines(List<String> instanceIps) {
		print("Symlinking skfsd on all machines");

		String target   = cloudOutDir + "/../../build/skfs-build/skfs-install/arch-output-area/skfsd";
		String linkName = cloudOutDir + "/../skfs/skfsd";
		for (String instanceIp : instanceIps)
			ssh(instanceIp, "ln -sv " + target + " " + linkName + "; ls " + target + "; ls " + linkName);
		
		printDone("");
	}
	
	private void printNextSteps() {
		System.out.println();
		System.out.println("Next steps: To start sk/skfs on all of these instances, you can run:");
		System.out.println("SKAdmin.sh -G " + cloudOutDir + " -g " + cloudGcName + " -c StartNodes,CreateSKFSns,CheckSKFS");
	}
	
	private void startInstances() {
		System.out.println("=== STARTING INSTANCES ===");
		MultiInstanceStarter starter = new MultiInstanceStarter(AmazonEC2ClientBuilder.defaultClient(), newKeyName);
		starter.run();
	}
	
	private void stopInstances() {
		System.out.println("=== STOPPING INSTANCES ===");
		MultiInstanceStopper stopper = new MultiInstanceStopper(AmazonEC2ClientBuilder.defaultClient(), newKeyName);
		stopper.run();
	}
	
	private void terminateInstances() {
		System.out.println("=== TERMINATING INSTANCES ===");
		MultiInstanceTerminator terminator = new MultiInstanceTerminator(AmazonEC2ClientBuilder.defaultClient(), newKeyName);
		terminator.run();
		stopZk();
	}
	
	private void stopZk() {
		print("Stopping ZooKeeper");

		runCmd(userHome + "/SilverKing/build/aws/zk_stop.sh");
		
		printDone("");
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
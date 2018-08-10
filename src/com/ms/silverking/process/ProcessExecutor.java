package com.ms.silverking.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Shell.ShellCommandExecutor;

import com.ms.silverking.time.TimeUtils;

public class ProcessExecutor {
	private String[] commands;
	private ShellCommandExecutor shellCommandExecutor;
	
	public ProcessExecutor(String[] commands) {
		this(commands, 0L/*0 timeout = no timeout*/);	
	}	
	
	public ProcessExecutor(String[] commands, long timeoutInSeconds) {
//		System.out.println("commands: " + Arrays.toString(commands));
		this.commands = commands;
		shellCommandExecutor = new ShellCommandExecutor(commands, null, null, TimeUtils.secondsInMillis((int)timeoutInSeconds));
	}
	
	public void execute() throws IOException {
		shellCommandExecutor.execute();
	}
	
	public String getOutput() {
		return shellCommandExecutor.getOutput();
	}
	
	public int getExitCode() {
		return shellCommandExecutor.getExitCode();
	}
	
	public boolean timedOut() {
		return shellCommandExecutor.isTimedOut();
	}
	
	public String[] getCommands() {
		return commands;
	}

	public static ProcessExecutor bashExecutor(String commands) {
		return bashExecutor(commands, 0L);
	}

	public static ProcessExecutor bashExecutor(String commands, long timeoutInSeconds) {
//		return new ProcessExecutor(new String[]{"/bin/bash", "-c", "'" + commands + "'"}, timeoutInSeconds);	// quotes messes it up
		return new ProcessExecutor(new String[]{"/bin/bash", "-c", commands}, timeoutInSeconds);
	}
	
	public static ProcessExecutor sshExecutor(String server, String commands) {
		return new ProcessExecutor( getSshCommandWithRedirectOutputFile(server, commands) );
	}
	
	/////////////////////////
	
	public static final String separator = File.separator;
	public static final String newline   = System.lineSeparator();
	
	public static void printDirContents(String header, String dirPath) {
		System.out.println("  === " + header);
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "ls -lR " + dirPath}) );
	}

	private static String runSumCmd(String cmd, File f) {
		String absPath = f.getAbsolutePath() + separator;
		System.out.println(absPath);
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\;"}));
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\; | sed s#"+absPath+"##"}));
		String out =        runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\; | sed s#"+absPath+"## | sort "});
		System.out.println(out);
		return out;
	}
	
	// useful for chained commands
	public static String runBashCmd(String commands) {
//		return runCmd(new String[]{"/bin/bash", "-c", "'" + commands + "'"});	// quotes messes it up
		return runCmd(new String[]{"/bin/bash", "-c", commands});
	}
	
	public static String runSshCmdWithRedirectOutputFile(String server, String commands) {
		return runCmd( getSshCommandWithRedirectOutputFile(server, commands) );
	}
	
	public static String ssh(String server, String commands) {
		return runCmd( getSshCommand(server, commands) );
	}
	
	public static void scpFile(String destDir, String file, String host) {
		String user = System.getProperty("user.name");
		runCmd("scp -o StrictHostKeyChecking=no " + file + " " + user + "@" + host + ":" + destDir);
	}
	
	public static String runCmd(String cmd, File f) {
		String[] commands = {cmd, f.getAbsolutePath()};
		return runCmd(commands);
	}
	
	public static String runCmd(String command) {
		return runCmd(command.split(" "));
	}

	private static String runCmd(String[] commands) {
		return runCmd(commands, true, true);
	}
	
	public static String runCmdNoWait(String command) {
		return runCmd(command.split(" "), true, false);
	}
	
	public static void runCmdNoWait(String[] commands) {
		runCmd(commands, false, false);
	}
	
	// https://stackoverflow.com/questions/5928225/how-to-make-pipes-work-with-runtime-exec
	//  - single commands can be run with:                                 
	//    "String"   - i.e. runTime.exec("ssh-keygen -y -f /home/ec2-user/.ssh/id_rsa")
	//  - chained commands, like pipe or cat or append, etc.. you have to use:
	//    "String[]" - i.e. runTime.exec("ssh-keygen -y -f /home/ec2-user/.ssh/id_rsa >> /home/ec2-user/.ssh/authorized_keys") fails. you have to do
	//                      runTime.exec(new String[]{"/bin/sh", "-c", "ssh-keygen -y -f /home/ec2-user/.ssh/id_rsa >> /home/ec2-user/.ssh/authorized_keys"})
	//
	// http://stackoverflow.com/questions/5711084/java-runtime-getruntime-getting-output-from-executing-a-command-line-program
	private static String runCmd(String[] commands, boolean captureOutput, boolean wait) {
//		System.out.println("commands: " + Arrays.toString(commands));
		StringBuffer output = new StringBuffer();
		Runtime runTime = Runtime.getRuntime();
		try {
			Process p;
			if (commands.length == 1)
				p = runTime.exec(commands[0]);
			else
				p = runTime.exec(commands);
			
			// maybe need to put this (capturing output) after the wait below?
			if (captureOutput) {
		        InputStream inputStream = p.getInputStream();
				BufferedReader stdInput = new BufferedReader(new InputStreamReader(inputStream));
				String line = null;
				while ((line = stdInput.readLine()) != null) 
					output.append(line + newline);
				inputStream.close();
			}
			
			if (wait) 
				p.waitFor();
			else
				p.waitFor(0, TimeUnit.SECONDS);
			
			if (0 != p.exitValue()) {
	        	System.out.println("Commands: " + Arrays.toString(commands) + "\nexited with code = " + p.exitValue());
	        	InputStream errorStream = p.getErrorStream();
	        	BufferedReader stdErr = new BufferedReader(new InputStreamReader(errorStream));
	        	String line = null;
	        	while ((line = stdErr.readLine()) != null)
	        		System.out.println(line);
	        	errorStream.close();
	        }
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		
		return output.toString();
	}
	
	public static String[] getSshCommandWithRedirectOutputFile(String server, String commands) {
//		return new String[]{"ssh -v -x -o StrictHostKeyChecking=no " + server + " \"/bin/bash -c '" + commands + "'\""};	// quotes are important around commands
//		return new String[]{"ssh", "-v", "-x", "-o", "StrictHostKeyChecking=no", server, "/bin/bash", "-c", "'" + commands + "'"};	// quotes are important around commands
		return new String[]{"ssh", "-v", "-x", "-o", "StrictHostKeyChecking=no", server, "/bin/bash -c '" + commands + "'", " > /tmp/ssh.out"};	// quotes are important around commands
	}
	
	public static String[] getSshCommand(String server, String commands) {
		return new String[]{"ssh", "-x", "-o", "StrictHostKeyChecking=no", server, "/bin/bash", "-c", "'" + commands + "'"};	// quotes are important around commands
	}
}

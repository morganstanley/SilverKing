package com.ms.silverking.process;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.Shell.ShellCommandExecutor;

import com.ms.silverking.time.TimeUtils;

public class ProcessExecutor {
	private String[] commands;
	private ShellCommandExecutor sce;
	
	public ProcessExecutor(String[] commands) {
		this(commands, 0L);	// 0 timeout = no timeout
	}	
	
	public ProcessExecutor(String[] commands, long timeoutInSeconds) {
//		System.out.println("commands: " + Arrays.toString(commands));
		this.commands = commands;
		sce = new ShellCommandExecutor(commands, null, null, TimeUtils.secondsInMillis((int)timeoutInSeconds));
	}
	
	public void execute() throws IOException {
		sce.execute();
	}
	
	public String getOutput() {
		return sce.getOutput();
	}
	
	public int getExitCode() {
		return sce.getExitCode();
	}
	
	public boolean timedOut() {
		return sce.isTimedOut();
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
		return new ProcessExecutor( getSshCommands(server, commands) );
	}
	
	/////////////////////////
	
	public static final String sep = File.separator;
	public static final String nl  = System.lineSeparator();
	
	public static void printDirContents(String header, String dirPath) {
		System.out.println("  === " + header);
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "ls -lR " + dirPath}) );
	}

	private static String runSumCmd(String cmd, File f) {
		String absPath = f.getAbsolutePath() + sep;
		System.out.println(absPath);
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\;"}));
		System.out.println( runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\; | sed s#"+absPath+"##"}));
		String out =        runCmd(new String[]{"/bin/sh", "-c", "find " + absPath + " -type f -exec " + cmd + " {} \\; | sed s#"+absPath+"## | sort "});
		System.out.println(out);
		return out;
	}
	
	public static String runBashCmd(String commands) {
//		return runCmd(new String[]{"/bin/bash", "-c", "'" + commands + "'"});	// quotes messes it up
		return runCmd(new String[]{"/bin/bash", "-c", commands});
	}
	
	public static String runSshCmd(String server, String commands) {
		return runCmd( getSshCommands(server, commands) );
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
	
	// http://stackoverflow.com/questions/5711084/java-runtime-getruntime-getting-output-from-executing-a-command-line-program
	private static String runCmd(String[] commands, boolean captureOutput, boolean wait) {
//		System.out.println("commands: " + Arrays.toString(commands));
		StringBuffer	output;
		
		output = new StringBuffer();
		Runtime rt = Runtime.getRuntime();
		try {
			Process p;
			if (commands.length == 1)
				p = rt.exec(commands[0]);
			else
				p = rt.exec(commands);
			
			if (captureOutput) {
				BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
				String s = null;
				while ((s = stdInput.readLine()) != null) { 
					output.append(s + nl);
				}
			}
			
			if (wait) 
				p.waitFor();
			else
				p.waitFor(0, TimeUnit.SECONDS);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		
		return output.toString();
	}
	
	public static String[] getSshCommands(String server, String commands) {
//		return new String[]{"ssh -v -x -o StrictHostKeyChecking=no " + server + " \"/bin/bash -c '" + commands + "'\""};	// quotes are important around commands
//		return new String[]{"ssh", "-v", "-x", "-o", "StrictHostKeyChecking=no", server, "/bin/bash", "-c", "'" + commands + "'"};	// quotes are important around commands
		return new String[]{"ssh", "-v", "-x", "-o", "StrictHostKeyChecking=no", server, "/bin/bash -c '" + commands + "'", " > /tmp/ssh.out"};	// quotes are important around commands
	}
}

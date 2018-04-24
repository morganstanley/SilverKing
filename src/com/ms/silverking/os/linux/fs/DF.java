package com.ms.silverking.os.linux.fs;

import java.io.IOException;

import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.process.ProcessExecutor;

public class DF {
	private static final String	dfCommand = "/bin/df";
	
	public static final int	noTimeout = 0;
	private static final int	blockSize = 1024; // df -k uses 1k blocks
	
	/*	
	 * Expect output of the form:
	 * Filesystem     1K-blocks      Used Available Use% Mounted on
	 * /dev/sda4      948658340 413875852 486586700  46% /
	 * 
	 */
	
	public static Triple<Long,Long,Integer> df(String path) throws IOException, RuntimeException {
		return df(path, noTimeout);
	}
	
	public static Triple<Long,Long,Integer> df(String path, int timeoutInSeconds) throws IOException, RuntimeException {
		ProcessExecutor	pe;
		String[]		commands;
		String			rawOutput;
		
		commands = new String[3];
		commands[0] = dfCommand;
		commands[1] = "-k";
		commands[2] = path;
		pe = new ProcessExecutor(commands, timeoutInSeconds);
		pe.execute();
		if (pe.getExitCode() != 0) {
			throw new RuntimeException("non-zero df exit code: "+ pe.getExitCode());
		}
		rawOutput = pe.getOutput();
		if (rawOutput == null) {
			throw new RuntimeException("null df output: ");
		} else {
			String[]	output;
			int			line2Index;
			
			line2Index = rawOutput.indexOf('/');
			if (line2Index < 0) {
				throw new RuntimeException("Unexpected df output: "+ rawOutput);
			} else {
				String		line2;
				
				line2 = rawOutput.substring(line2Index);
				output = line2.split("\\s+");
				try {
					long	totalBlocks;
					long	usedBlocks;
					
					totalBlocks = Long.parseLong(output[1]);
					usedBlocks = Long.parseLong(output[2]);
					return new Triple<>(totalBlocks, usedBlocks, blockSize);
				} catch (RuntimeException re) {
					Log.logErrorWarning(re);
					throw new RuntimeException("Exception parsing output: "+ rawOutput, re);
				}
			}
		}
	}

	public static void main(String[] args) {
		try {
			Triple<Long,Long,Integer>	df;
			
			df = df(args[0]);
			if (df != null) {
				System.out.printf("Total: %d\n", df.getV1());
				System.out.printf("Used:  %d\n", df.getV2());
				System.out.printf("Free:  %d\n", df.getV1() - df.getV2());
			} else {
				System.out.printf("Error running df\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

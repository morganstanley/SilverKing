package com.ms.silverking.net;

import java.net.Socket;
import java.util.concurrent.TimeUnit;

import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;

public class WaitForHostPort {
	private static final int	successExitCode = 0;
	private static final int	errorExitCode = -1;
	private static final int	pollIntervalMillis = 1 * 1000;
	
	private static boolean canConnect(HostAndPort hostAndPort) {
		try {
			Socket	s;
			
			s = new Socket(hostAndPort.getHostName(), hostAndPort.getPort());
			s.close();
			return true;
		} catch (Exception e) {
			Log.fine(e);
			return false;
		}
	}
	
	public static int doWait(HostAndPort hostAndPort, int timeoutSeconds) {
		Timer	sw;
		
		sw = new SimpleTimer(TimeUnit.SECONDS, timeoutSeconds);
		while (!sw.hasExpired()) {
			if (canConnect(hostAndPort)) {
				return successExitCode;
			}
			ThreadUtil.sleep(pollIntervalMillis);
		}
		return errorExitCode;
	}

	public static void main(String[] args) {
		int			exitCode;
		
		exitCode = errorExitCode;
		if (args.length != 2) {
			System.err.println("args: <hostAndPort> <timeoutSeconds>");
		} else {
			HostAndPort	hostAndPort;
			int			timeoutSeconds;
			
			hostAndPort = new HostAndPort(args[0]);
			timeoutSeconds = Integer.parseInt(args[1]);
			try {
				exitCode = doWait(hostAndPort, timeoutSeconds);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.exit(exitCode);
	}
}

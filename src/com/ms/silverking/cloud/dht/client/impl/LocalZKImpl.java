package com.ms.silverking.cloud.dht.client.impl;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;

import com.ms.silverking.io.FileUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.ParseExceptionAction;

public class LocalZKImpl {
	private static final int	presumedSuccessTimeMillis = 2 * 1000;
	
	private static int	minStartPort = 10000;
	private static int	maxStartPort = 12000;
	private static int	maxAttempts = 1000;
	
	public static final String	zkPortProperty = LocalZKImpl.class.getName() +".ZKPort";
    public static final int		defaultZKPort = 0;
	private static final int	zkPort;
	
	static {
		zkPort = PropertiesHelper.systemHelper.getInt(zkPortProperty, defaultZKPort, ParseExceptionAction.RethrowParseException);
	}	
	
	public static int startLocalZK(String dataDirBase) {
		int	port;
		boolean	success;
		
		success = false;
		if (zkPort == 0) {
			port = ThreadLocalRandom.current().nextInt(minStartPort, maxStartPort);
			for (int i = 0; i < maxAttempts; i++) {
				if (startLocalZK(port, dataDirBase +"/"+ port)) {
					success = true;
					break;
				}
			}
		} else {
			port = zkPort;
			if (startLocalZK(port, dataDirBase +"/"+ port)) {
				success = true;
			}
		}
		if (success) {
			return port;
		} else {
			throw new RuntimeException("LocalZKImpl.startLocalZK() failed");
		}
	}
	
	public static boolean startLocalZK(int port, String dataDir) {
		return new LocalZKImpl()._startLocalZK(port, dataDir);
	}
	
	private LocalZKImpl() {
	}
	
	private boolean _startLocalZK(int port, String dataDirName) {
		Starter	starter;
		File	dataDir;
		
		dataDir = new File(dataDirName);
		dataDir.mkdirs();
		FileUtil.cleanDirectory(dataDir);
		starter = new Starter(port, dataDirName);
		return starter.waitForStartup();
	}
	
	private class Starter implements Runnable {
		private final int		port;
		private final String	dataDir;
		private boolean	failed;
		
		Starter(int port, String dataDir) {
			this.port = port;
			this.dataDir = dataDir;
			new Thread(this, "LocalZKImpl.Starter").start();
		}
		
		private void startLocalZK(int port, String dataDir) {
			String[]	args;
			
			args = new String[2];
			args[0] = Integer.toString(port);
			args[1] = dataDir;
			org.apache.zookeeper.server.quorum.QuorumPeerMain.main(args);
		}
		
		public void run() {
			startLocalZK(port, dataDir);
			failed = true;
		}
		
		public boolean waitForStartup() {
			ThreadUtil.sleep(presumedSuccessTimeMillis);
			return !failed;
		}
	}

	public static void main(String[] args) {
		String	dataDirBase;
		
		dataDirBase = args[0];
		System.out.printf("ZK started on port %d\n", startLocalZK(dataDirBase));
		/*
		org.apache.zookeeper.server.quorum.QuorumPeerMain
		if [ "x$2" != "x" ]
				then
				    ZOOCFG=$ZOOCFGDIR/$2
				fi
				echo "Using config: $ZOOCFG"
		
		start)
	    echo  "Starting zookeeper ... "
	    java  "-Dzookeeper.log.dir=${ZOO_LOG_DIR}" "-Dzookeeper.root.logger=${ZOO_LOG4J_PROP}" \
	    -cp $CLASSPATH $JVMFLAGS $ZOOMAIN $ZOOCFG >$ZOOLOG &
	    echo $! > $ZOOPIDFILE
	    echo STARTED
	    ;;
	*/
	}
}

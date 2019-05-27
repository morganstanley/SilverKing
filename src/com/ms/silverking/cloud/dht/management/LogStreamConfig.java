package com.ms.silverking.cloud.dht.management;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.ClassVars;
import com.ms.silverking.cloud.dht.meta.ClassVarsZK;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.log.Log;


/**
 * Enable redirection of stdout/stderr within the JVM
 */
public class LogStreamConfig {
	public static void configureLogStreams(SKGridConfiguration gc, String logFileName) throws IOException, KeeperException {
		MetaClient			dhtMC;
		DHTConfiguration	dhtConfig;
		ClassVarsZK			classVarsZK;
		ClassVars			defaultClassVars;
        PrintStream			logStream;
        File				logDir;
		
		dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(gc);
		dhtConfig = dhtMC.getDHTConfiguration();
		classVarsZK = new ClassVarsZK(dhtMC);
		if (dhtConfig.getDefaultClassVars() != null) {
			defaultClassVars = DHTConstants.defaultDefaultClassVars.overrideWith(classVarsZK.getClassVars(dhtConfig.getDefaultClassVars()));
		} else {
			defaultClassVars = DHTConstants.defaultDefaultClassVars;
		}
		
        
        logDir = new File(DHTConstants.getSKInstanceLogDir(defaultClassVars, gc));
        Log.warning("Ensuring created: ", logDir);
        logDir.mkdirs();
        logStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(logDir, logFileName))), true);
        System.setOut(logStream);
        System.setErr(logStream);
        Log.setPrintStreams(logStream);
	}
}

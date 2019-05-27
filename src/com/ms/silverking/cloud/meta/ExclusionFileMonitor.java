package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.LogStreamConfig;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfigurationUtil;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

public class ExclusionFileMonitor {
    private ExclusionZK exclusionZK;
    private final int	watchIntervalSeconds;
    private final File	exclusionFile;
    
    private static final String	logFileName = "ExclusionFileMonitor.out";
    
    public ExclusionFileMonitor(SKGridConfiguration gc, int watchIntervalSeconds, String exclusionFile) throws IOException, KeeperException {
    	MetaClient	mc;
        NamedRingConfiguration  ringConfig;
    	
        this.watchIntervalSeconds = watchIntervalSeconds;
        this.exclusionFile = new File(exclusionFile);
		Log.warning("exclusionFile %s\n", exclusionFile);

        ringConfig = NamedRingConfigurationUtil.fromGridConfiguration(gc);
        
        mc = new MetaClient(ringConfig.getRingConfiguration().getCloudConfiguration(), gc.getClientDHTConfiguration().getZKConfig());

        exclusionZK = new ExclusionZK(mc);
    }
    
    public void monitor() {
    	while (true) {
    		try {
    			updateCloudExclusionSet();
    			ThreadUtil.sleepSeconds(watchIntervalSeconds);
    		} catch (Exception e) {
    			Log.logErrorWarning(e);
    		}
    	}
    }
    
    private void updateCloudExclusionSet() {
        try {
	    	ExclusionSet	exclusionSet;
	    	ExclusionSet	newExclusionSet;
	        Set<String>		excludedInFiles;
	        
	        excludedInFiles = getCurrentlyExcludedInFiles();
	        Log.warning(String.format("Excluded in files:"));
	        Log.warning(String.format("%s\n", 
	                CollectionUtil.toString(excludedInFiles)));
	        exclusionSet = exclusionZK.readLatestFromZK();
	        newExclusionSet = new ExclusionSet(excludedInFiles, -1, exclusionSet.getMzxid());
	        if (!newExclusionSet.equals(exclusionSet)) {
				exclusionZK.writeToZK(newExclusionSet);
	        }
		} catch (Exception e) {
            Log.logErrorWarning(e, "Exception in updateCloudExclusionSet");
		}
    }
    
	private Set<String> getCurrentlyExcludedInFiles() throws IOException {
		return exclusionZK.readFromFile(exclusionFile, 0).getServers();
	}
	
    //////////////////////
	
	public static void main(String[] args) {
        try {
            CmdLineParser       parser;
            ExclusionFileMonitorOptions    options;
            ExclusionFileMonitor   exclusionFileMonitor;
            SKGridConfiguration gc;
            
            options = new ExclusionFileMonitorOptions();
            parser = new CmdLineParser(options);
            try {
                parser.parseArgument(args);
                gc = SKGridConfiguration.parseFile(options.gridConfig);
                LogStreamConfig.configureLogStreams(gc, logFileName);
                exclusionFileMonitor = new ExclusionFileMonitor(gc, 
                                                  options.watchIntervalSeconds,
                                                  options.exclusionFile);
                exclusionFileMonitor.monitor();
            } catch (CmdLineException cle) {
                System.err.println(cle.getMessage());
                parser.printUsage(System.err);
	            System.exit(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
	}
}

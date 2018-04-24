package com.ms.silverking.cloud.skfs.management;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.skfs.meta.MetaClient;
import com.ms.silverking.cloud.skfs.meta.MetaPaths;
import com.ms.silverking.cloud.skfs.meta.SKFSConfiguration;
import com.ms.silverking.cloud.skfs.meta.SKFSConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;

public class MetaUtil {
    private final ZooKeeperConfig    zkConfig;
    private final MetaClient         mc;
    private final String             skfsConfigName;
    private static final boolean     debug = false;
    
    public MetaUtil(String skfsConfigName, String zkString) throws KeeperException,  IOException {
        zkConfig = new ZooKeeperConfig(zkString);
        mc = new MetaClient(skfsConfigName, zkConfig);
        this.skfsConfigName = skfsConfigName;
    }

    private long getLatestVersion(String path) throws KeeperException {
        return mc.getZooKeeper().getLatestVersion(path);
    }
    
    private long getVersionPriorTo_floored(String path, long zkid) throws KeeperException {
        long    version;
        
        version = mc.getZooKeeper().getVersionPriorTo(path, zkid);
        return version == -1 ? 0 : version;
    }
    
    private void getFromZK(long skfsVersion, File target) throws KeeperException, IOException {
    	
    	String conf = mc.getSKFSConfig();
        if (debug) {
        	System.err.printf("%s\t%d\n", MetaPaths.getConfigPath(skfsConfigName), skfsVersion);
        	System.err.printf("%s\n", conf);
        }
    	
    	
        //FIXME: use of getLatestVersion (not getVersionPriorTo_floored) as it is independent of SK versioning
        if(MetaUtilOptions.dhtVersionUnspecified == skfsVersion) {
        	skfsVersion = getLatestVersion(MetaPaths.getConfigPath(skfsConfigName));
        }
        if (debug) {
        	System.err.printf("%s\t%d\n", MetaPaths.getConfigPath(skfsConfigName), skfsVersion);
        }
        SKFSConfigurationZK skfsConfigZk = new SKFSConfigurationZK(mc);
        SKFSConfiguration skfsConfig= skfsConfigZk.readFromZK(skfsVersion, null);
        if (debug) {
        	System.err.printf("SKFSConfiguration\t%s\n", skfsConfig.getConfig());
        }
        skfsConfigZk.writeToFile(target, skfsConfig);
    }
    
    private void LoadFromFile(long skfsVersion, File target) throws KeeperException, IOException {
        //FIXME: do I need below to handle dhtVersionUnspecified or is it automatic?
    	/*
        if(MetaUtilOptions.dhtVersionUnspecified == skfsVersion) {
        	skfsVersion = getLatestVersion(MetaPaths.getConfigPath(skfsConfigName));
        	skfsVersion++;
        }
        */
        if (debug) {
        	System.err.printf("%s\t%d\n", MetaPaths.getConfigPath(skfsConfigName), skfsVersion);
        }
        SKFSConfigurationZK skfsConfigZk = new SKFSConfigurationZK(mc);
        SKFSConfiguration skfsConfig= skfsConfigZk.readFromFile(target, skfsVersion);
        if (debug) {
        	System.err.printf("SKFSConfiguration\t%s\n", skfsConfig.getConfig());
        }
        skfsConfigZk.writeToZK(skfsConfig, null);
    }
    
    
    public void run(MetaUtilOptions options) throws KeeperException, IOException {
    	File target = options.targetFile == null ? null : new File(options.targetFile);
        switch (options.command) {
        case GetFromZK:
        	getFromZK(options.skfsVersion, target);
            break;
        case LoadFromFile:
        	LoadFromFile(options.skfsVersion, target);
            break;
        	
        default: throw new RuntimeException("panic");
        }
    }


    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            MetaUtil        mu;
            MetaUtilOptions options;
            CmdLineParser   parser;
            
            options = new MetaUtilOptions();
            parser = new CmdLineParser(options);
            try {
                parser.parseArgument(args);
            } catch (CmdLineException cle) {
                System.err.println(cle.getMessage());
                parser.printUsage(System.err);
    			System.exit(-1);
            }
            
            if (options.gridConfigName == null) {
            	if (options.skfsConfigName == null) {
            		System.err.println("One of -d or -g must be specified");
            	} else {
            		options.gridConfigName = options.skfsConfigName;
            	}
            }
            
            mu = new MetaUtil(options.gridConfigName, options.zkEnsemble) ;
            mu.run(options);
        } catch (Exception e){
            e.printStackTrace();
			System.exit(-1);
        }
    }
    
}

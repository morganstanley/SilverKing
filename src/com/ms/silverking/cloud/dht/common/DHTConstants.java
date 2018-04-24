package com.ms.silverking.cloud.dht.common;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.ms.silverking.SKConstants;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions.Mode;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.WaitForTimeoutController;
import com.ms.silverking.cloud.dht.client.crypto.AESEncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.XOREncrypterDecrypter;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.ClassVars;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;


/**
 * DHTConstants for internal use. Not exposed to clients.
 */
public class DHTConstants {
    public static final long noSuchVersion = Long.MIN_VALUE;
    public static final long	unspecifiedVersion = 0;
    
    public static final byte[]	emptyByteArray = new byte[0];
    
    public static final StorageType            defaultStorageType = StorageType.FILE;
    public static final ConsistencyProtocol    defaultConsistencyProtocol = ConsistencyProtocol.TWO_PHASE_COMMIT;
    public static final NamespaceVersionMode   defaultVersionMode = NamespaceVersionMode.SINGLE_VERSION;
    public static final RevisionMode           defaultRevisionMode = RevisionMode.NO_REVISIONS;
    public static final int                    defaultSegmentSize = 64 * 1024 * 1024;
    public static final int                    defaultSecondarySyncIntervalSeconds = 30 * 60;
    public static final int                    defaultSecondaryReplicaUpdateTimeoutMillis = 2 * 60 * 1000;
    public static final SegmentIndexLocation   defaultSegmentIndexLocation = SegmentIndexLocation.RAM;
    public static final int					   defaultNSPrereadGB = 0;
    public static final int					   defaultMinPrimaryUnderFailure = 1;
    
    public static final int noCapacityLimit = -1;
    public static final int defaultFileSegmentCacheCapacity = noCapacityLimit;
    
    private static final int				   defaultReapInterval = 10;
    public static final StorageModule.RetrievalImplementation	defaultRetrievalImplementation = StorageModule.RetrievalImplementation.Ungrouped;
    
    public static final String  systemClassBase = "com.ms.silverking";
    public static final String	daemonPackageBase = systemClassBase +".cloud.dht.daemon";
	    
	public static final String	daemonLogFile = "Daemon.out";
	public static final String	stopSKFSLogFile = "StopSKFS.out";
	public static final String	checkSKFSLogFile = "CheckSKFS.out";
	public static final String	prevDaemonLogFile = "Daemon.out.1";
	public static final String	heapDumpFile = "DHTNode.heap";
	public static final ClassVars	defaultDefaultClassVars;

	public static final String	initialHeapSizeVar = "initialHeapSize";
	public static final String	maxHeapSizeVar = "maxHeapSize";
	public static final String	dataBaseVar = "dataBase";
	public static final String	ipAliasMapFileVar = "skIPAliasMapFile";
	public static final String	ipAliasMapFileEnvVar = "skIPAliasMapFile";
	public static final String	dataBasePathProperty = daemonPackageBase +".DataBasePath";
	public static final String	skInstanceLogBaseVar = "skInstanceLogBase";
	public static final String	skDaemonJavaCommandHeaderVar = "skDaemonJavaCommandHeader";
	public static final String	skDaemonJavaCommandHeaderEnvVar = "skDaemonJavaCommandHeader";
	public static final String	killCommandVar = "killCommand";
	public static final String	killCommandEnvVar = "skKillCommand";
	public static final String	clearDataCommandVar = "clearDataCommand";
	public static final String	checkSKFSCommandVar = "checkSKFSCommand";
	public static final String	checkSKFSCommandEnvVar = "skCheckSKFSCommand";	
	public static final String	reapIntervalVar = "reapInterval";
	public static final String	reapIntervalProperty = daemonPackageBase +".ReapInterval";
	public static final String	fileSegmentCacheCapacityVar = "fileSegmentCacheCapacity";
	public static final String	fileSegmentCacheCapacityProperty = daemonPackageBase +".FileSegmentCacheCapacity";
	public static final String	retrievalImplementationVar = "retrievalImplementation";
	public static final String	retrievalImplementationProperty = daemonPackageBase +".RetrievalImplementation";
	public static final String	segmentIndexLocationVar = "segmentIndexLocation";
	public static final String	segmentIndexLocationProperty = daemonPackageBase +".SegmentIndexLocation";
	public static final String	nsPrereadGBVar = "nsPrereadGB";
	public static final String	nsPrereadGBProperty = daemonPackageBase +".NSPrereadGB";
	
	public static final String	ssSubDirName = "ss";
	
	
	public static final String classpathEnv = "SK_CLASSPATH";
	public static final String classpathProperty = "java.class.path";
	public static final String jaceHomeEnv = "SK_JACE_HOME";
	public static final String javaHomeEnv = SKConstants.javaHomeEnv;
	public static final String javaHomeProperty = "java.home";
	
	public static boolean	isDaemon = false;
	
	static {
		Map<String,String>	defMap;
		
		defMap = new HashMap<>();
		defMap.put(initialHeapSizeVar, "1024");
		defMap.put(maxHeapSizeVar, "1024");
		defMap.put(ipAliasMapFileVar, PropertiesHelper.envHelper.getString(ipAliasMapFileEnvVar, ""));
		defMap.put(dataBaseVar, "/var/tmp/silverking/data");
		defMap.put(skInstanceLogBaseVar, "/tmp/silverking");
		defMap.put(skDaemonJavaCommandHeaderVar, PropertiesHelper.envHelper.getString(skDaemonJavaCommandHeaderEnvVar, ""));
		defMap.put(killCommandVar, PropertiesHelper.envHelper.getString(killCommandEnvVar, UndefinedAction.ZeroOnUndefined));
		defMap.put(clearDataCommandVar, "rm -rf");
		defMap.put(checkSKFSCommandVar, PropertiesHelper.envHelper.getString(checkSKFSCommandEnvVar, UndefinedAction.ZeroOnUndefined));
		defMap.put(reapIntervalVar, Integer.toString(defaultReapInterval));
		defMap.put(retrievalImplementationVar, defaultRetrievalImplementation.toString());
		defMap.put(fileSegmentCacheCapacityVar, Integer.toString(defaultFileSegmentCacheCapacity));
		defMap.put(segmentIndexLocationVar, defaultSegmentIndexLocation.toString());
		defMap.put(nsPrereadGBVar, Integer.toString(defaultNSPrereadGB));
		defaultDefaultClassVars = new ClassVars(defMap, 0);
	}
	
	public static String getSKInstanceLogDir(ClassVars classVars, SKGridConfiguration gc) {
		return classVars.getVarMap().get(DHTConstants.skInstanceLogBaseVar)
					+"/"+ gc.getClientDHTConfiguration().getName();
	}
    
	public static final Set<SecondaryTarget> noSecondaryTargets = null;
    public static final OpTimeoutController	standardTimeoutController = new OpSizeBasedTimeoutController();
    public static final OpTimeoutController	standardWaitForTimeoutController = new WaitForTimeoutController();
    /** 
     * Standard PutOptions. Subject to change. Recommended practice is for each SilverKing instance to specify 
     * an instance default (using NamespaceCreationOptions.defaultNSOptions.defaultPutOptions.) 
     */
    public static final PutOptions standardPutOptions = new PutOptions(
                                                    standardTimeoutController, 
                                                    noSecondaryTargets, Compression.LZ4, 
                                                    ChecksumType.MURMUR3_32, 
                                                    false, 
                                                    PutOptions.defaultVersion, null); // FUTURE - FOR NOW THIS MUST BE REPLICATED IN PUT OPTIONS, parse limitation
    public static final InvalidationOptions standardInvalidationOptions = OptionsHelper.newInvalidationOptions(standardTimeoutController, InvalidationOptions.defaultVersion, noSecondaryTargets);
    public static final GetOptions standardGetOptions = OptionsHelper.newGetOptions(
            standardTimeoutController, RetrievalType.VALUE, VersionConstraint.defaultConstraint);
    public static final WaitOptions standardWaitOptions = OptionsHelper.newWaitOptions(
            RetrievalType.VALUE, VersionConstraint.defaultConstraint, 
            Integer.MAX_VALUE, WaitOptions.THRESHOLD_MAX);
    
    public static final NamespaceOptions    defaultNamespaceOptions = OptionsHelper.newNamespaceOptions(defaultStorageType, 
                                                defaultConsistencyProtocol, defaultVersionMode, defaultRevisionMode,
                                                standardPutOptions, standardInvalidationOptions, 
                                                standardGetOptions, standardWaitOptions, 
                                                defaultSecondarySyncIntervalSeconds, defaultSegmentSize);
    public static final NamespaceOptions	dynamicNamespaceOptions = defaultNamespaceOptions.storageType(StorageType.RAM)
                                          .consistencyProtocol(ConsistencyProtocol.TWO_PHASE_COMMIT)
                                          .versionMode(NamespaceVersionMode.CLIENT_SPECIFIED);
    
                                                
   /**
    * Default NamespaceCreationOptions. Subject to change. Recommended practice is for each SilverKing instance
    * to explicitly specify. 
    */
    public static final NamespaceCreationOptions   defaultNamespaceCreationOptions 
                                        = new NamespaceCreationOptions(Mode.OptionalAutoCreation_AllowMatches, "^_.*", 
                                                defaultNamespaceOptions);
    
    // default encryption
    public static final String				defaultEncrypterDecrypterProperty = systemClassBase +".DefaultEncrypterDecrypter";
    public static final EncrypterDecrypter	defaultDefaultEncrypterDecrypter = null;
    public static final EncrypterDecrypter	defaultEncrypterDecrypter;
    
    static {
    	String	val;
    	
    	val = PropertiesHelper.systemHelper.getString(defaultEncrypterDecrypterProperty, UndefinedAction.ZeroOnUndefined);
    	if (val == null) {
    		defaultEncrypterDecrypter = defaultDefaultEncrypterDecrypter;
    	} else {
    		try {
	    		if (val.equals(AESEncrypterDecrypter.name) || val.equals(AESEncrypterDecrypter.class.getName())) {
	    			defaultEncrypterDecrypter = new AESEncrypterDecrypter();
	    		} else if (val.equals(XOREncrypterDecrypter.name) || val.equals(XOREncrypterDecrypter.class.getName())) {
	    			defaultEncrypterDecrypter = new XOREncrypterDecrypter();
	    		} else {
	    			throw new RuntimeException("Unknown EncrypterDecrypter: "+ val);
	    		}
    		} catch (Exception e) {
    			throw new RuntimeException("Exception initializing EncrypterDecrypter", e);
    		}
    	}
    }
    
    // Creation time functionality
    // here only because SystemTimeUtil needs to access this
    public static final long    nanoOriginTimeInMillis;
    
    static {
    	/*
        Calendar    c;
        
        c = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.set(2000, 0, 1, 0, 0, 0);
        nanoOriginTimeInMillis = c.getTimeInMillis();
        */
    	// Above code has error. Below removes this error and ensures no skew across runs/instances
    	nanoOriginTimeInMillis = 946684800000L;
    }   
}

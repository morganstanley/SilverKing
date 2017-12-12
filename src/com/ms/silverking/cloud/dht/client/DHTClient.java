package com.ms.silverking.cloud.dht.client;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.client.impl.DHTSessionImpl;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.DHTNode;
import com.ms.silverking.cloud.dht.daemon.DHTNodeConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.MetaPaths;
import com.ms.silverking.cloud.dht.meta.StaticDHTCreator;
import com.ms.silverking.cloud.toporing.TopoRingConstants;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AsyncGlobals;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;


/**
 * Base client interface to DHT functionality. Provides sessions
 * to specific DHT instances.
 */
public class DHTClient {
	private final Lock				       sessionCreationLock;
	private final Map<String,DHTSession>   dhtNameToSessionMap;
	private final SerializationRegistry    serializationRegistry;
	
	private static final double	concurrentExtraThreadFactor = 1.25;
	private static final double	nonConcurrentExtraThreadFactor = 1.0;
	
	private static ValueCreator    valueCreator;
	
	private static final AbsMillisTimeSource   absMillisTimeSource;
	
    private static final int defaultInactiveNodeTimeoutSeconds = 30;	
	
	/*
	private static void initLog4j() {  
		Logger rootLogger = Logger.getRootLogger();  
		if (!rootLogger.getAllAppenders().hasMoreElements()) {      
			rootLogger.setLevel(org.apache.log4j.Level.WARN);      
			rootLogger.addAppender(new ConsoleAppender(             
					new PatternLayout("%-5p [%t]: %m%n")));
		}
	}
	*/

    private static final int   defaultClientWorkUnit = 16;
	
	static {
        Log.initAsyncLogging();
	    AsyncGlobals.setVerbose(false);
	    TopoRingConstants.setVerbose(false);
        LWTPoolProvider.createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters().workUnit(defaultClientWorkUnit).ignoreDoubleInit(true));
		//initLog4j();
        //WorkPoolProvider.createWorkPools(concurrentExtraThreadFactor, nonConcurrentExtraThreadFactor, true);
        //if (!Log.levelMet(Level.INFO)) {
        //	AsyncServer.verbose = false;
        //}
	    valueCreator = SimpleValueCreator.forLocalProcess();
	    absMillisTimeSource = new TimerDrivenTimeSource();
	    OutgoingData.setAbsMillisTimeSource(absMillisTimeSource);
	}
	
	/**
	 * Return the ValueCreator in use
	 * @return the ValueCreator in use
	 */
	public static ValueCreator getValueCreator() {
	    return valueCreator;
	}
	
	/**
	 * Construct DHTClient with the specified SerializationRegistry. 
	 * @throws IOException
	 */
	@OmitGeneration
	public DHTClient(SerializationRegistry serializationRegistry) throws IOException {
		sessionCreationLock = new ReentrantLock();
		dhtNameToSessionMap = new HashMap<>();
		this.serializationRegistry = serializationRegistry;
	}

	/**
     * Construct DHTClient with default SerializationRegistry. 
	 * @throws IOException
	 */
    public DHTClient() throws IOException {
        this(SerializationRegistry.createDefaultRegistry());
    }
    
	/**
	 * Open a new session to the specified SilverKing DHT instance using default SessionOptions.
	 * @param dhtConfigProvider specifies the SilverKing DHT instance 
	 * @return a new session to the given instance
	 * @throws ClientException
	 */
    public DHTSession openSession(ClientDHTConfigurationProvider dhtConfigProvider) throws ClientException {        
        return openSession(new SessionOptions(dhtConfigProvider.getClientDHTConfiguration()));
    }
    
    /**
     * Open a new session to the specified SilverKing DHT instance using the given SessionOptions.
     * @param sessionOptions options specifying the SilverKing DHT instance and the parameters of this session
     * @return a new session to the given instance
     * @throws ClientException
     */
	public DHTSession openSession(SessionOptions sessionOptions) throws ClientException {
	    ClientDHTConfiguration dhtConfig;
	    String preferredServer;
	    
	    dhtConfig = sessionOptions.getDHTConfig();
	    preferredServer = sessionOptions.getPreferredServer();
	    
	    if (preferredServer != null) {
	    	if (preferredServer.equals(SessionOptions.EMBEDDED_PASSIVE_NODE)) {
	    		embedPassiveNode(dhtConfig);
		    	preferredServer = null;
	    	} else if (preferredServer.equals(SessionOptions.EMBEDDED_KVS)) {
	    		String	gcBase;
	    		String	gcName;
	    		
	    		dhtConfig = embedKVS();
	    		gcBase = "/tmp"; // FIXME - make user configurable
	    		gcName = "GC_"+ dhtConfig.getName();
	    		try {
	    			Log.warningf("GridConfigBase: %s", gcBase);
	    			Log.warningf("GridConfigName: %s", gcName);
					StaticDHTCreator.writeGridConfig(dhtConfig, gcBase, gcName);
				} catch (IOException e) {
					throw new ClientException("Error creating embedded kvs", e);
				}
		    	preferredServer = null;
	    	}
	    }
	    
		sessionCreationLock.lock();
		try {			
			DHTSession   session;
			
			if (preferredServer == null) {
			    preferredServer = IPAddrUtil.localIPString();
			}
			//session = dhtNameToSessionMap.get(dhtConfig.getName());
			session = null; // FUTURE - this forces a new session
			                // think about whether we want to use multiple or cache to common
			if (session == null) {
				DHTSession  prev;
				int         serverPort;

				try {
				    if (dhtConfig.hasPort()) {
                        serverPort = dhtConfig.getPort();
                    } else {
    				    MetaClient      mc;
    				    MetaPaths       mp;
    				    long            latestConfigVersion;

    				    Log.warning("dhtConfig.getZkLocs(): "+ dhtConfig.getZKConfig());
    				    mc = new MetaClient(dhtConfig);
    				    mp = mc.getMetaPaths();
    				    
    				    Log.warning("getting latest version: "+ mp.getInstanceConfigPath());
    				    latestConfigVersion = mc.getZooKeeper().getLatestVersion(mp.getInstanceConfigPath()); 
    				    Log.warning("latestConfigVersion: "+ latestConfigVersion);
    				    serverPort = new DHTConfigurationZK(mc).readFromZK(latestConfigVersion, null).getPort();
				    }
				} catch (Exception e) {
				    throw new ClientException(e);
				}
				try {
				    session = new DHTSessionImpl(dhtConfig,
				        new IPAndPort(IPAddrUtil.serverNameToAddr(preferredServer), serverPort),
				        absMillisTimeSource, serializationRegistry, sessionOptions.getTimeoutController());
                } catch (IOException ioe) {
                    throw new ClientException(ioe);
				}
				Log.info("session returned: ", session);
				/*
				prev = dhtNameToSessionMap.put(dhtConfig.getName(), session);
				if (prev != null) {
				    throw new RuntimeException("panic");
				}
				*/
			}
			return session;
		} finally {
			sessionCreationLock.unlock();
		}
	}

	private void embedPassiveNode(ClientDHTConfiguration dhtConfig) {
		DHTNode	embeddedNode;
		Path	tempDir;
		File	skDir;
		
		try {
			tempDir = Files.createTempDirectory(null);
			skDir = new File(tempDir.toFile(), "silverking");
			skDir.mkdirs();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		
		DHTNodeConfiguration.setDataBasePath(skDir.getAbsolutePath() +"/data");
		embeddedNode = new DHTNode(dhtConfig.getName(), dhtConfig.getZKConfig(), defaultInactiveNodeTimeoutSeconds, false, false);
	}
	
	private ClientDHTConfiguration embedKVS() {
		return EmbeddedSK.createEmbeddedSKInstance();
	}
}

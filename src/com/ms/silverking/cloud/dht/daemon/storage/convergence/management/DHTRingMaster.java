package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingIDAndVersionPair;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.management.CentralConvergenceController.SyncTargets;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.LogStreamConfig;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationListener;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationWatcher;
import com.ms.silverking.cloud.dht.meta.DHTMetaReader;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.MetaPaths;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupReceiver;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.ExclusionZK;
import com.ms.silverking.cloud.meta.ServerSetExtensionZK;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.toporing.meta.RingChangeListener;
import com.ms.silverking.cloud.toporing.meta.RingConfigWatcher;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Quadruple;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.numeric.NumUtil;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.thread.lwt.BaseWorker;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SystemTimeSource;

/**
 * Observers ZooKeeper for changes in metadata that this DHT is dependent upon.
 * 
 * Currently, this comprises two types of data: the DHT configuration, and
 * the ring tree (only one currently supported) that this DHT uses.
 */
public class DHTRingMaster implements DHTConfigurationListener, RingChangeListener, MessageGroupReceiver, RequestController {
    private final MetaClient  mc;
    private final MetaPaths   mp;
    private final ZooKeeperConfig   zkConfig;
    private final DHTConfigurationWatcher	dhtConfigWatcher;
    private volatile RingConfigWatcher      ringConfigWatcher;
    private final long        intervalMillis;
    private volatile DHTConfiguration   dhtConfig;
    private final boolean   enableLogging;
    private final RingChangedWorker	ringChangedWorker;
    private final RecoverDataWorker	recoverDataWorker;
    private ConvergenceControllerBase	targetConvergenceController;
    private final DHTMetaReader	dhtMetaReader;
    private Mode	mode;
    private ConcurrentMap<UUIDBase,ConvergenceControllerBase>	convergenceControllers;
    
    private final MessageGroupBase	mgBase;
    
    // FUTURE - BELOW IS TEMP DEV PLACEHOLDER
    private final DHTRingCurTargetZK    dhtRingCurTargetZK;
	private Triple<String,Long,Long>	currentRing;
	private Triple<String,Long,Long>	targetRing;
	private ConvergencePoint			currentCP;
	private ConvergencePoint			targetCP;
	
	private final Lock	convergenceLock;
    
    private static final String noTargetName = "";
    private static final long   noTargetVersion = -1;
    
    private static final String	logFileName = "DHTRingMaster.out";
    
    private static final int	zkReadAttempts = 90;
    private static final int	zkReadRetryIntervalMillis = 1 * 1000;
    
    private static final int	initializationCheckIntervalMillis = 1000;
    private static final int	cccPollIntervalMillis = 100;
    
    private static final int	mgBasePort = 0; // Use ephemeral port
    
    private static final int	numSelectorControllers = Runtime.getRuntime().availableProcessors();
    private static final String	selectorControllerClass = DHTRingMaster.class.getName();
    private static final int	queueLimit = 1024;

    // We shouldn't need to sync unchanged owners. For now, we are paranoid and do it.
	private static final boolean	syncUnchangedOwners = true;
    
    private static final boolean	debugValidTarget = true;
    
    static {
    	OutgoingData.setAbsMillisTimeSource(SystemTimeSource.instance);
    }

    public DHTRingMaster(ZooKeeperConfig zkConfig, String dhtName, 
                          long intervalMillis, Mode mode, boolean enableLogging) throws IOException, KeeperException, AlreadyBoundException {
        mc = new MetaClient(dhtName, zkConfig);
        convergenceControllers = new ConcurrentHashMap<>();
        this.intervalMillis = intervalMillis;
        this.mode = mode;
        mp = mc.getMetaPaths();
        this.zkConfig = zkConfig;
        convergenceLock = new ReentrantLock();
        /*
         * The below VersionWatcher watches the DHTConfiguration for changes. Whenever a new DHTConfiguration
         * is observed, a new RingConfigWatcher is started.
         */
        dhtConfigWatcher = new DHTConfigurationWatcher(zkConfig, dhtName, intervalMillis, enableLogging);
        this.enableLogging = enableLogging;
        
        dhtRingCurTargetZK = new DHTRingCurTargetZK(mc, dhtConfig);
        
        ringChangedWorker = new RingChangedWorker();
        recoverDataWorker = new RecoverDataWorker();
        
        dhtMetaReader = new DHTMetaReader(zkConfig, dhtName, enableLogging);
        
        mgBase = new MessageGroupBase(mgBasePort, this, SystemTimeUtil.systemTimeSource, null, 
        							  queueLimit, numSelectorControllers, selectorControllerClass);
        mode = Mode.Automatic;
        new RingMasterControlImpl(this);
    }
    
    public DHTRingMaster(ZooKeeperConfig zkConfig, String dhtName, long intervalMillis, Mode mode) 
                  throws IOException, KeeperException, AlreadyBoundException {
        this(zkConfig, dhtName, intervalMillis, mode, true);
    }
    
    public String getDHTName() {
    	return mc.getDHTName();
    }
    
    public void setMode(Mode mode) {
		convergenceLock.lock();
    	try {
    		this.mode = mode;
    		Log.warningAsync("setMode: ", mode);
    	} finally {
    		convergenceLock.unlock();
    	}
    }
    
    public Mode getMode() {
		convergenceLock.lock();
    	try {
    		return mode;
    	} finally {
    		convergenceLock.unlock();
    	}
    }
    
    /**
     * Wait until the dhtConfigWatcher has read in a configuration.
     * Watch for changes to the configuration after it is set up.
     */
    private void waitForInitialDHTConfiguration() {
    	Log.warningAsync("Waiting for initial DHTConfiguration");
    	dhtConfigWatcher.waitForDHTConfiguration();
    	dhtConfig = dhtConfigWatcher.getDHTConfiguration();
    	Log.warningAsync("Received initial DHTConfiguration: ", dhtConfig);
    	dhtConfigWatcher.addListener(this);
    	newDHTConfiguration(dhtConfig);
    }
    
    private ExclusionSet readExclusions(Quadruple<String, Long, Long, Long> ring) throws KeeperException, IOException {
    	return readExclusions(ring.getHead(), ring.getV2());
    }
    
    private ExclusionSet readExclusions(String ringName, long ringVersion) throws KeeperException, IOException {
    	ExclusionZK		exclusionZK;
        ExclusionSet	instanceExclusionSet;
        ExclusionSet	exclusionSet;        
        com.ms.silverking.cloud.meta.MetaClient  cloudMC;
        com.ms.silverking.cloud.toporing.meta.MetaClient	ringMC;
        
        ringMC = com.ms.silverking.cloud.toporing.meta.MetaClient.createMetaClient(ringName, ringVersion, zkConfig);
        cloudMC = ringMC.createCloudMC();
        
        exclusionZK = new ExclusionZK(cloudMC);
        try {
            exclusionSet = exclusionZK.readLatestFromZK();
        } catch (Exception e) {
        	Log.logErrorWarning(e);
        	Log.warningAsync("No ExclusionSet found. Using empty set.");
        	exclusionSet = ExclusionSet.emptyExclusionSet(0);
        }
        
        try {
        	instanceExclusionSet = new ExclusionSet(new ServerSetExtensionZK(mc, mc.getMetaPaths().getInstanceExclusionsPath()).readLatestFromZK());
        } catch (Exception e) {
        	Log.logErrorWarning(e);
        	Log.warningAsync("No instance ExclusionSet found. Using empty set.");
        	instanceExclusionSet = ExclusionSet.emptyExclusionSet(0);
        }
        return ExclusionSet.union(exclusionSet, instanceExclusionSet);
    }
        
    /**
     * Ensure that ring pointers are set up correctly upon startup.
     * @throws KeeperException
     */
    private void checkPointers() throws KeeperException {
    	boolean	initialized;
    	
    	initialized = false;
    	while (!initialized) {
	    	currentRing = dhtRingCurTargetZK.getCurRingAndVersionPair();
	    	targetRing = dhtRingCurTargetZK.getTargetRingAndVersionPair();
	    	
	    	if (currentRing == null) {
	    		if (targetRing == null) {
	    			// Neither current nor target ring set. We must wait for a ring to initialize.
	    			Log.warningAsync("No current ring. Waiting...");
	    			ThreadUtil.sleep(initializationCheckIntervalMillis);
	    		} else {
	    			// Current is not set, but target is. Bad state.
	    			throw new RuntimeException();
	    		}
	    	} else {
    			Log.warningAsync("currentRing: ", currentRing);
    			Log.warningAsync("targetRing: ", targetRing);
	    		initialized = true;
	    		if (targetRing == null) {
	    			// Current is set, no ongoing convergence. No further initialization required.
	    		} else {
	    			// Current is set, ongoing convergence. Restart the old convergence.
	    		}
	    	}
    	}
    }
    
    public void start() throws Exception {
    	waitForInitialDHTConfiguration();
    	setCurrentRing();
    	checkPointers();
    }
  
    public ZooKeeperConfig getZKConfig() {
        return zkConfig;
    }
    
    public MetaClient getMetaClient() {
        return mc;
    }
    
    public DHTConfiguration getDHTConfiguration() {
        return dhtConfig;
    }
    
    //////////////////////////////////////////////////
    // begin DHTConfigurationListener implementation
    
    /**
     * Called when a new DHT configuration is found. Reads the configuration and starts a ring configuration
     * watcher for it. Stops the old ring configuration watcher if there is one.
     */
    @Override
    public void newDHTConfiguration(DHTConfiguration dhtConfig) {
        try {
            if (enableLogging) {
                Log.warningAsync("DHTRingMaster.newDHTConfiguration(): ", dhtConfig);
            }
            convergenceLock.lock();
            try {
                this.dhtConfig = dhtConfig;
                startRingConfigWatcher(dhtConfig.getRingName());
            } finally {
            	convergenceLock.unlock();
            }
        } catch (Exception e) {
            Log.logErrorWarning(e);
        }
    }
    
    /**
     * Start a new ring configuration watcher, stopping the old if it exists.
     */
    private void startRingConfigWatcher(String ringName) {
        if (enableLogging) {
            Log.warningAsync("DHTRingMaster.startRingConfigWatcher(): ", ringName);
        }
        try {
            if (ringConfigWatcher != null) {
                ringConfigWatcher.stop();
            }
            ringConfigWatcher = new RingConfigWatcher(zkConfig, ringName, intervalMillis, enableLogging, this);
        } catch (Exception e) {
            Log.logErrorWarning(e);
        }
    }
    
    // end DHTConfigurationListener implementation 
    ////////////////////////////////////////////////
    
    
    ////////////////////////////////////////////
    // begin RingChangeListener implementation
    
    private boolean isValidTarget(Quadruple<String,Long,Long,Long> r) {
    	// dhtConfig.getRingName() should match the target ring
    	if (!r.getV1().equals(dhtConfig.getRingName())) {
    		if (debugValidTarget) {
    			Log.warningAsyncf("ring name mismatch %s %s", r.getV1(), dhtConfig.getRingName());
    		}
    		return false;
    	} else {
    		if (currentRing == null) {
    			return true;
    		} else {
	    		if (!r.getV1().equals(currentRing.getV1())) {
	    			// Target ring name doesn't match current ring name
	    			// Can't say much here
	    			return true;
	    		} else {
	    			if (mode == Mode.Automatic) {
	    				boolean	versionsIncrease;
	    				
		        		// The target ring name matches the current ring name
		        		// The versions should increase
	    				versionsIncrease = NumUtil.compare(currentRing.getTail(), r.getPairAt2()) < 0;
	    	    		if (debugValidTarget) {
	    	    			if (!versionsIncrease) {
	    	    				Log.warningAsyncf("Versions don't increase %s %s. Not allowed in Automatic mode.", currentRing.getTail(), r.getPairAt2());
	    	    			}
	    	    		}
	    				return versionsIncrease;
	    			} else {
	    				// In manual mode allow anything
	    				return true;
	    			}
	    		}
    		}
    	}
    }
    
    @Override
    public void ringChanged(String ringName, String basePath, Pair<Long,Long> ringVersion, long creationTimeMillis) {
    	if (mode == Mode.Automatic) {
    		ringChangedWorker.addWork(new Quadruple<>(UUIDBase.random(), Quadruple.of(ringName, ringVersion, creationTimeMillis),null,SyncTargets.Primary));
    	} else {
    		Log.warningAsyncf("mode == Manual. Ignoring new ring: %s %s", ringName, ringVersion);
    	}
    }
    
    private class RingChangedWorker extends BaseWorker<Quadruple<UUIDBase,Quadruple<String,Long,Long,Long>,Quadruple<String,Long,Long,Long>,SyncTargets>> {
		@Override
		public void doWork(Quadruple<UUIDBase,Quadruple<String, Long, Long, Long>,Quadruple<String, Long, Long, Long>,SyncTargets> idAndRings) {
			if (idAndRings.getV3() == null) {
				setTarget(idAndRings.getPairAt1());
			} else {
				syncData(idAndRings);
			}
		}
    }
    
    public void setTarget(Pair<UUIDBase,Quadruple<String,Long,Long,Long>> idAndRing) {
    	UUIDBase							uuid;
    	Quadruple<String,Long,Long,Long>	ring;
    	
    	uuid = idAndRing.getV1();
    	ring = idAndRing.getV2();
		Log.warningAsyncf("DHTRingMaster.setTarget %s %s", uuid, ring.getV1());
    	convergenceLock.lock();
		try {
			if (!isValidTarget(ring)) {
	        	// FUTURE - could improve this sanity check to look at zk info
	        	Log.warningAsync("Ignoring invalid target ring: ", ring);
			} else {
	    		if (currentRing == null) {
	    			try {
	    				currentRing = ring.getTripleAt1();
						dhtRingCurTargetZK.setCurRingAndVersionPair(ring.getV1(), ring.getPairAt2());
					} catch (KeeperException ke) {
						Log.logErrorWarning(ke);
					}
	    		} else {
			        if (enableLogging) {
			            Log.warningAsync("New ring: ", ring);
			        }
			        try {
				        CentralConvergenceController.RequestedSyncMode	syncMode;
				        
						// Stop old convergence
						if (targetConvergenceController != null) {
					        if (enableLogging) {
					            Log.warningAsync("Abandoning old convergence: ", targetRing);
					        }
							targetConvergenceController.abandon();
						}
						
						// Start new convergence
						dhtRingCurTargetZK.setTargetRingAndVersionPair(ring.getV1(), ring.getPairAt2());
						targetRing = ring.getTripleAt1();
						targetCP = new ConvergencePoint(dhtConfig.getVersion(), RingIDAndVersionPair.fromRingNameAndVersionPair(ring.getTripleAt1()), ring.getV4());
				        if (enableLogging) {
				            Log.warningAsync("New targetRing: ", targetRing);
				        }
				        
				        if (mode == Mode.ManualNoSync) {
				        	syncMode = CentralConvergenceController.RequestedSyncMode.SetStateOnly;
				        } else {
				        	syncMode = CentralConvergenceController.RequestedSyncMode.SyncAndSetStateUnlessSubset;
				        }
			        	// MAKE syncUnchangedOnwers CONFIGURABLE
				        
						try {
							targetConvergenceController = new CentralConvergenceController(uuid, dhtMetaReader, currentCP, targetCP, readExclusions(ring), mgBase, syncUnchangedOwners, syncMode);
							convergenceControllers.put(targetConvergenceController.getUUID(), targetConvergenceController);
						} catch (IOException ioe) {
							Log.logErrorWarning(ioe, "Unable to start convergence");
							return;
						}
						try {
							convergenceLock.unlock();
							try {
						        if (enableLogging) {
						            Log.warningAsync("Calling converge(): ", targetRing);
						        }
								((CentralConvergenceController)targetConvergenceController).converge(CentralConvergenceController.SyncTargets.Primary);
							} finally {
								convergenceLock.lock();
							}
							
							// Convergence was successful
							// Set the cur -> target, target -> null?
							currentRing = targetRing;
							currentCP = targetCP;
							dhtRingCurTargetZK.setCurRingAndVersionPair(currentRing.getHead(), currentRing.getTail());
							targetRing = null;
							targetCP = null;
				            targetConvergenceController = null;
					        if (enableLogging) {
					            Log.warningAsync("Convergence complete: ", targetRing);
					        }
						} catch (ConvergenceException ce) {
							// Convergence failed
							if (!ce.getAbandoned()) {
								// Failed due to an exception
								Log.logErrorWarning(ce, "Convergence failed due to exception");
							} else {
								// Failed due to a new target causing the old convergence to be abandoned
								// In this case, the new convergence will take over the cur and target pointers
								Log.warningAsync("Previous convergence abandoned");
							}
						}
					} catch (KeeperException ke) {
						// Retries are internal to DHTringCurTargetZK. If we got here, those
						// retries failed.
						// FUTURE - relay to alerting
						Log.logErrorWarning(ke, "Unexpected exception during peerStateMet handling");
					}
	    		}
    		}
    	} finally {
    		convergenceLock.unlock();
    	}
    }
    
    private boolean ringIsSubset(Triple<String, Long, Long> superRing, Triple<String, Long, Long> subRing) {
		// TODO Auto-generated method stub
		return false;
	}

	public void syncData(Quadruple<UUIDBase,Quadruple<String,Long,Long,Long>,Quadruple<String,Long,Long,Long>,SyncTargets> idAndRings) {
    	UUIDBase							uuid;
    	Quadruple<String,Long,Long,Long>	sourceRing;
    	Quadruple<String,Long,Long,Long>	targetRing;
    	
    	uuid = idAndRings.getV1();
    	sourceRing = idAndRings.getV2();
    	targetRing = idAndRings.getV3();
		Log.warningAsyncf("DHTRingMaster.syncData %s %s %s", uuid, sourceRing.getV1(), targetRing.getV1());
    	convergenceLock.lock();
		try {
			if (/*!isValidTarget(targetRing)*/false) { // FUTURE think about checking validity
	        	// FUTURE - could improve this sanity check to look at zk info
	        	Log.warningAsyncf("Ignoring invalid sync %s %s", sourceRing, targetRing);
			} else {
		        if (enableLogging) {
		            Log.warningAsync("New ring: ", targetRing);
		        }
		        try {
		        	ConvergencePoint	_sourceCP;
		        	ConvergencePoint	_targetCP;
		        	
					// Check for old convergence
					if (targetConvergenceController != null) {
				        if (enableLogging) {
				            Log.warningAsync("Can't sync due to ongoing convergence: ", targetRing);
				            return;
				        }
					}
					
					// Start new sync
					_sourceCP = new ConvergencePoint(dhtConfig.getVersion(), RingIDAndVersionPair.fromRingNameAndVersionPair(sourceRing.getTripleAt1()), sourceRing.getV4());
					_targetCP = new ConvergencePoint(dhtConfig.getVersion(), RingIDAndVersionPair.fromRingNameAndVersionPair(targetRing.getTripleAt1()), targetRing.getV4());
					try {
						targetConvergenceController = new CentralConvergenceController(uuid, dhtMetaReader, _sourceCP, _targetCP, readExclusions(targetRing), mgBase, syncUnchangedOwners, CentralConvergenceController.RequestedSyncMode.SyncOnly);
						convergenceControllers.put(targetConvergenceController.getUUID(), targetConvergenceController);
					} catch (IOException ioe) {
						Log.logErrorWarning(ioe, "Unable to start sync");
						return;
					}
					try {
						convergenceLock.unlock();
						try {
					        if (enableLogging) {
					            Log.warningAsync("Calling converge(): ", targetRing);
					        }
							((CentralConvergenceController)targetConvergenceController).converge(idAndRings.getV4());
						} finally {
							convergenceLock.lock();
						}
						
			            targetConvergenceController = null;
				        if (enableLogging) {
				            Log.warningAsync("Convergence complete: ", targetRing);
				        }
					} catch (ConvergenceException ce) {
						// Convergence failed
						if (!ce.getAbandoned()) {
							// Failed due to an exception
							Log.logErrorWarning(ce, "Convergence failed due to exception");
						} else {
							// Failed due to a new target causing the old convergence to be abandoned
							// In this case, the new convergence will take over the cur and target pointers
							Log.warningAsync("Previous convergence abandoned");
						}
					}
				} catch (KeeperException ke) {
					// Retries are internal to DHTringCurTargetZK. If we got here, those
					// retries failed.
					// FUTURE - relay to alerting
					Log.logErrorWarning(ke, "Unexpected exception during peerStateMet handling");
				}
    		}
    	} finally {
    		convergenceLock.unlock();
    	}
    }
    
    private long getCreationTime(String ringName, long configVersion, long instance) {
		long	creationTime;
		int		attemptIndex;
		
        creationTime = Long.MIN_VALUE;
        attemptIndex = 0;
        do {
        	try {
        		String	basePath;
        		
        		basePath = com.ms.silverking.cloud.toporing.meta.MetaPaths.getRingConfigPath(ringName);
				creationTime = mc.getZooKeeper().getCreationTime(ZooKeeperExtended.padVersionPath(basePath, configVersion) +"/instance/"+ ZooKeeperExtended.padVersion(instance));
			} catch (KeeperException e) {
				Log.logErrorWarning(e);
			}
        	if (creationTime < 0) {
        		ThreadUtil.sleep(zkReadRetryIntervalMillis);
        	}
        	++attemptIndex;
        } while (creationTime < 0 && attemptIndex < zkReadAttempts);
        return creationTime;
    }
    
    private void waitForConvergenceControllerCreation(UUIDBase uuid) {
    	do {
    		ThreadUtil.sleep(cccPollIntervalMillis);
    	} while (!convergenceControllers.containsKey(uuid));
    }
    
	public UUIDBase setTarget(Triple<String, Long, Long> target) {
		String	ringName;
		long	creationTime;
		UUIDBase	uuid;
		
		Log.warningAsyncf("DHTRingMaster.setTarget %s", target);
		ringName = target.getHead();
		creationTime = getCreationTime(ringName, target.getTail().getV1(), target.getTail().getV2());
        if (creationTime < 0) {
        	Log.warningAsync("Ignoring setTarget() due to zk exceptions: "+ ringName +" "+ target.getTail());
        	return null;
        }
        uuid = UUIDBase.random();
		ringChangedWorker.addWork(new Quadruple<>(uuid, Quadruple.of(target, creationTime), null, SyncTargets.Primary), 0);
		waitForConvergenceControllerCreation(uuid);
		return uuid;
	}
	
	public UUIDBase syncData(Triple<String, Long, Long> source, Triple<String, Long, Long> target, SyncTargets syncTargets) {
		String		sourceRingName;
		long		sourceCreationTime;
		String		targetRingName;
		long		targetCreationTime;
		UUIDBase	uuid;
		
		Log.warningAsyncf("DHTRingMaster.syncData %s %s", source, target);
		sourceRingName = source.getHead();
		sourceCreationTime = getCreationTime(sourceRingName, target.getTail().getV1(), target.getTail().getV2());
        if (sourceCreationTime < 0) {
        	Log.warningAsync("Ignoring syncData() due to zk exceptions: "+ sourceRingName +" "+ source.getTail());
        	return null;
        }
		targetRingName = target.getHead();
		targetCreationTime = getCreationTime(targetRingName, target.getTail().getV1(), target.getTail().getV2());
        if (targetCreationTime < 0) {
        	Log.warningAsync("Ignoring syncData() due to zk exceptions: "+ targetRingName +" "+ target.getTail());
        	return null;
        }
        uuid = UUIDBase.random();
		ringChangedWorker.addWork(new Quadruple<>(uuid, Quadruple.of(source, sourceCreationTime), Quadruple.of(target, targetCreationTime), syncTargets), 0);
		waitForConvergenceControllerCreation(uuid);
		return uuid;
	}
	
	public UUIDBase recoverData() {
		UUIDBase	uuid;
		
		Log.warningAsyncf("DHTRingMaster.recoverData");
        uuid = UUIDBase.random();
        recoverDataWorker.addWork(uuid);
		return uuid;
	}
	   
    // end RingChangeListener implementation
    //////////////////////////////////////////
    
	private void setCurrentRing() throws KeeperException {
		Triple<String,Long,Long>	currentRing;
		
		Log.warningAsync("Reading current ring");
		currentRing = dhtRingCurTargetZK.getCurRingAndVersionPair();
		Log.warningAsyncf("Setting current ring: %s", currentRing);
        setCurrentRing(dhtConfig, currentRing);
		Log.warningAsync("Current ring set");
	}

    private void setCurrentRing(DHTConfiguration _dhtConfig, Triple<String,Long,Long> ring) {
    	setCurrentRing(_dhtConfig, Quadruple.of(ring, getCreationTime(ring.getHead(), ring.getV2(), ring.getV3())));
    }
    
    private void setCurrentRing(DHTConfiguration _dhtConfig, Quadruple<String,Long,Long,Long> ring) {
		currentCP = new ConvergencePoint(_dhtConfig.getVersion(), RingIDAndVersionPair.fromRingNameAndVersionPair(ring.getTripleAt1()), ring.getV4());
    }
	
	
    ////////////////////////////////////////
    // MessageGroupReceiver implementation
    
	@Override
	public void receive(MessageGroup message, MessageGroupConnection connection) {
		ConvergenceControllerBase	_targetConvergenceController;
		
		System.out.println(connection);
		System.out.println(message);
		_targetConvergenceController = targetConvergenceController;
		if (_targetConvergenceController == null) {
			switch (message.getMessageType()) {
			case CHECKSUM_TREE:
				displayChecksumTree(message, connection);
				break;
			default:
				Log.info("Ignoring message due to null _targetConvergenceController");
			}
		} else {
			switch (message.getMessageType()) {
			case NAMESPACE_RESPONSE:
				_targetConvergenceController.handleNamespaceResponse(message, connection);
				break;
			case OP_RESPONSE:
				_targetConvergenceController.handleOpResponse(message, connection);
				break;
			case PROGRESS:
				_targetConvergenceController.handleProgress(message, connection);
				break;
			case CHECKSUM_TREE:
				displayChecksumTree(message, connection);
				break;
			default:
				Log.warningAsync("Ignoring unexpected MessageType: ", message.getMessageType());
			}
		}
	}

    // End MessageGroupReceiver implementation
    ////////////////////////////////////////////
	
	public ConvergencePoint cpFromTriple(Triple<String, Long, Long> t) {
		String		ringName;
		long		creationTime;
		
		ringName = t.getHead();
		creationTime = getCreationTime(ringName, t.getTail().getV1(), t.getTail().getV2());
        if (creationTime < 0) {
        	Log.warningAsync("Ignoring cpFromTriple() due to zk exceptions: "+ ringName +" "+ t.getTail());
        	return null;
        }
		return new ConvergencePoint(dhtConfig.getVersion(), RingIDAndVersionPair.fromRingNameAndVersionPair(t), creationTime);
	}
	
	public void requestChecksumTree(Triple<Long,Long,Long> nsAndRegion, Triple<String, Long, Long> source, Triple<String, Long, Long> target, String owner) {
		requestChecksumTree(nsAndRegion, cpFromTriple(source), cpFromTriple(target), new IPAndPort(owner));
	}
	
	public void requestChecksumTree(Triple<Long,Long,Long> nsAndRegion, ConvergencePoint curCP, ConvergencePoint targetCP, IPAndPort owner) {		
        MessageGroup	mg;
        UUIDBase		uuid;
        
		uuid = UUIDBase.random();
        mg = new ProtoChecksumTreeRequestMessageGroup(uuid, nsAndRegion.getV1(), targetCP, curCP,  
                                                      mgBase.getMyID(), new RingRegion(nsAndRegion.getV2(), nsAndRegion.getV3()), mgBase._getIPAndPort(), false).toMessageGroup(); 
        mgBase.send(mg, owner);
	}
	
	public void displayChecksumTree(MessageGroup message, MessageGroupConnection connection) {
        ChecksumNode		remoteTree;
        ConvergencePoint	cp;
        
        cp = ProtoChecksumTreeMessageGroup.getConvergencePoint(message);
        remoteTree = ProtoChecksumTreeMessageGroup.deserialize(message);
        System.out.printf("incomingChecksumTree: %s\t%s\t%s\n", message.getUUID(), cp, connection);
		System.out.println(remoteTree);
	}
	
	
	///////////////////////////////
	// RecoverData implementation
	
    private class RecoverDataWorker extends BaseWorker<UUIDBase> {
		@Override
		public void doWork(UUIDBase uuid) {
			recoverData(uuid);
		}
    }
    
    public void recoverData(UUIDBase uuid) {
		Log.warningAsyncf("DHTRingMaster.recoverData %s", uuid);
    	convergenceLock.lock();
		try {
    		if (currentRing == null) {
    			Log.warning("No current ring. Unable to recover data.");
    		} else {
    	    	try {
					// Check for ongoing
					if (targetConvergenceController != null) {
			            Log.warningAsync("Can't recover data due to ongoing convergence: ", targetRing);
					} else {
	        	    	RecoverDataController	recoverDataController;
	        	    	
						recoverDataController = new RecoverDataController(uuid, dhtMetaReader, currentCP, readExclusions(currentRing.getHead(), currentRing.getV2()), mgBase);
						targetConvergenceController = recoverDataController;
						try {
							//convergenceLock.unlock();
							//try {
						        if (enableLogging) {
						            Log.warningAsync("Calling recover()");
						        }
						        recoverDataController.recover();
							//} finally {
							//	convergenceLock.lock();
							//}
						} catch (ConvergenceException ce) {
							// Convergence failed
							if (!ce.getAbandoned()) {
								// Failed due to an exception
								Log.logErrorWarning(ce, "Convergence failed due to exception");
							} else {
								// Failed due to a new target causing the old convergence to be abandoned
								// In this case, the new convergence will take over the cur and target pointers
								Log.warningAsync("Previous convergence abandoned");
							}
						}
					}
				} catch (KeeperException ke) {
					throw new RuntimeException(ke);
				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
    		}
    	} finally {
    		convergenceLock.unlock();
    	}
    }
	
	// End RecoverData implementation
	///////////////////////////////////
	
    public static void main(String[] args) {
        try {
            CmdLineParser       	parser;
            DHTRingMasterOptions	options;
            SKGridConfiguration		gc;
            DHTRingMaster			dhtRingMaster;
            
            Log.initAsyncLogging();
            options = new DHTRingMasterOptions();
            parser = new CmdLineParser(options);
            try {
            	int	intervalMillis;
            	
                parser.parseArgument(args);
                
                gc = SKGridConfiguration.parseFile(options.gridConfig);
                LogStreamConfig.configureLogStreams(gc, logFileName);
                
                intervalMillis = options.watchIntervalSeconds * 1000;
                
    	        LWTPoolProvider.createDefaultWorkPools();                
                
                dhtRingMaster = new DHTRingMaster(gc.getClientDHTConfiguration().getZKConfig(), 
                									gc.getClientDHTConfiguration().getName(), intervalMillis, options.mode);
                Log.warningAsync("DHTRingMaster created");
                dhtRingMaster.start();
                Log.warningAsync("DHTRingMaster running");
                ThreadUtil.sleepForever();
            } catch (CmdLineException cle) {
                System.err.println(cle.getMessage());
                parser.printUsage(System.err);
	            System.exit(-1);
            }        	
        } catch (Exception e) {
            e.printStackTrace();
            Log.logErrorSevere(e, DHTRingMaster.class.getName(), "main");
            System.exit(-1);
        }
    }
    
    /////////////////////////////////////////////////////////////
    
	@Override
	public void stop(UUIDBase uuid) {
	}

	@Override
	public void waitForCompletion(UUIDBase uuid) {
	}

	@Override
	public RequestStatus getStatus(UUIDBase uuid) {
    	ConvergenceControllerBase	ccc;

    	ccc = convergenceControllers.get(uuid);
    	if (ccc == null) {
    		return null;
    	} else {
    		return ccc.getStatus(uuid);
    	}
	}
	
    /////////////////////////////////////////////////////////////
	
	public void reap() {
	}

	public UUIDBase getCurrentConvergenceID() {
		if (targetConvergenceController != null) {
			return targetConvergenceController.getUUID();
		} else {
			return null;
		}
	}	
}

package com.ms.silverking.cloud.dht.daemon;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingIDAndVersionPair;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingState;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTMetaReader;
import com.ms.silverking.cloud.dht.meta.DHTMetaUpdate;
import com.ms.silverking.cloud.dht.meta.DHTMetaUpdateListener;
import com.ms.silverking.cloud.dht.meta.DHTRingCurTargetZK;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.ProtoOpResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoSetConvergenceStateMessageGroup;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.toporing.PrimarySecondaryIPListPair;
import com.ms.silverking.cloud.toporing.RingEntry;
import com.ms.silverking.cloud.toporing.meta.MetaPaths;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.thread.ThreadUtil;

/**
 * NodeRingMaster is responsible for responding to new target and current rings
 * that are set by DHTRingMaster.
 */
public class NodeRingMaster2 implements DHTMetaUpdateListener {
	private final String			dhtName;
    private final ZooKeeperConfig   zkConfig;
    private final IPAndPort         nodeID;
    private final ValueCreator		myOriginatorID;
	private final DHTMetaReader		dhtMetaReader;	
    private StorageModule           storageModule;
    private ExclusionSet      		latestExclusionSet;
    private boolean                 convergenceEnabled;
    private PeerHealthMonitor		peerHealthMonitor;
    private ReplicaPrioritizer		replicaPrioritizer;
    
    private volatile RingMapState2  curMapState;
    private volatile RingMapState2  targetMapState;
    private ConcurrentMap<RingIDAndVersionPair,RingMapState>    mapStates;
    
    private final Lock      mapLock;
    private final Condition mapCV;
    
    private static final boolean    debug = true;
    
    public NodeRingMaster2(String dhtName, ZooKeeperConfig zkConfig, IPAndPort nodeID) {
    	this.dhtName = dhtName;
        this.zkConfig = zkConfig;
        this.nodeID = nodeID;
        myOriginatorID = SimpleValueCreator.forLocalProcess();
        mapLock = new ReentrantLock();
        mapCV = mapLock.newCondition();
        mapStates = new ConcurrentHashMap<>();
        latestExclusionSet = ExclusionSet.emptyExclusionSet(0);
        try {
        	dhtMetaReader = new DHTMetaReader(zkConfig, dhtName);
        } catch (Exception e) {
        	throw new RuntimeException("Unexpected exception", e);
        }
    }
    
    public void setStorageModule(StorageModule storageModule) {
        this.storageModule = storageModule;
    }
    
    public void setPeerHealthMonitor(PeerHealthMonitor peerHealthMonitor) {
    	this.peerHealthMonitor = peerHealthMonitor;
        replicaPrioritizer = new ReplicaHealthPrioritizer(peerHealthMonitor);
    }
    
	@Override
	public void dhtMetaUpdate(DHTMetaUpdate dhtMetaUpdate) {
		dhtMetaReader.setDHTConfig(dhtMetaUpdate.getDHTConfig());
	}
    
	/*
    // FUTURE - CONSIDER EXCLUSION SET IN BELOW?
    // PROBABLY HOLD ON TO THE ORIGINAL MAP, AND COMPUTE NEW MAP MINUS EXCLUSIONS WHENEVER THE EXCLUSION
    // LIST IS UPDATED
    
	@Override
	public void newCurRingAndVersion(Triple<String,Long,Long> curRingAndVersionPair) {
		RingMapState			mapState;
		RingIDAndVersionPair	ringIDAndVersionPair;
		
		ringIDAndVersionPair = RingIDAndVersionPair.fromRingNameAndVersionPair(curRingAndVersionPair);
        mapLock.lock();
        try {
            if (curMapState == null) {
    			DHTMetaUpdate	dhtMetaUpdate;
    			
    			System.out.println("curRingAndVersionPair: "+ curRingAndVersionPair);
				dhtMetaUpdate = dhtMetaReader.readRing(curRingAndVersionPair);
    			mapState = createMapState(dhtMetaUpdate, true);            	
            	
    			mapStates.put(ringIDAndVersionPair, mapState);
                System.out.println("\tInitial Resolved Map Map");
                mapState.getResolvedReplicaMap().display();
                System.out.println("\tEnd Initial Resolved Map");
                System.out.println();
            } else {
            	// FIXME - Need to delete, but if convergence unsuccessful, then we need it for later
        		//mapState = mapStates.remove(ringIDAndVersionPair);
        		mapState = mapStates.get(ringIDAndVersionPair);
        		if (mapState == null) {
        			Log.warning("Unexpected can't find RingMapState for ringIDAndVersionPair in in mapStates: "+ ringIDAndVersionPair);
        			return;
        		}
        		curMapState.discard();
            }
            Log.warning("curMapState <-- ", mapState.getRingIDAndVersionPair());
			curMapState = mapState;
            mapCV.signalAll();
		} catch (Exception e) {
			Log.logErrorWarning(e, "Exception in newCurRingAndVersion");
        } finally {
            mapLock.unlock();
        }
	}

	@Override
	public void newTargetRingAndVersion(Triple<String,Long,Long> targetRingAndVersionPair) {
		RingIDAndVersionPair	targetRingIDAndVersionPair;
		
		Log.warning("in newTargetRingAndVersion: ", targetRingAndVersionPair);
		targetRingIDAndVersionPair = RingIDAndVersionPair.fromRingNameAndVersionPair(targetRingAndVersionPair);
		mapLock.lock();
		try {
			if (!convergenceEnabled) {
				// FIXME - throw exception?
			} else {
				if (targetRingIDAndVersionPair.equals(curMapState.getRingIDAndVersionPair())) {
					Log.warning("Ignoring (1) newTargetRingAndVersionID with ringIDAndVersionPair same as curMapState");
					return;
				} else {
					DHTMetaUpdate	dhtMetaUpdate;
					RingMapState	newMapState;
		            RingIDAndVersionPair    ringIDAndVersionPair;
					
		    		Log.warning("newTargetRingAndVersion: ", targetRingAndVersionPair);
					dhtMetaUpdate = dhtMetaReader.readRing(targetRingAndVersionPair);
					if (dhtMetaUpdate.getRingIDAndVersionPair().equals(curMapState.getRingIDAndVersionPair())) {
						Log.warning("Ignoring (2) newTargetRingAndVersion with ringIDAndVersionPair same as curMapState");
						return;
					}
					newMapState = createMapState(dhtMetaUpdate, false);
					if (newMapState != null) {
			            ringIDAndVersionPair = dhtMetaUpdate.getRingIDAndVersionPair();
			            mapStates.put(ringIDAndVersionPair, newMapState); 
			            processMapForConvergence(newMapState);
					} else {
						Log.warning("Map state not created");
					}
				}
			}
		} catch (Exception e) {
			Log.logErrorWarning(e, "Exception in newTargetRingAndVersion");
		} finally {
			mapLock.unlock();
			Log.warning("out newTargetRingAndVersion: ", targetRingAndVersionPair);
		}
	}

    public RingMapState2 createMapState(DHTMetaUpdate dhtMetaUpdate, boolean convergenceAlreadyComplete) {
        try {
            StoragePolicyGroup      storagePolicyGroup;
            StoragePolicyGroupZK    storagePolicyGroupZK;
            RingConfiguration       ringConfig;
            com.ms.silverking.cloud.dht.meta.MetaClient	dhtMC;
            com.ms.silverking.cloud.toporing.meta.MetaClient ringMC;
            long    storagePolicyGroupVersion;
            long    zkidLimit;
            ZooKeeperExtended   zk;
            ResolvedReplicaMap  resolvedReplicaMap;
            InstantiatedRingTree            ringTree;
            RingID              ringID;
            ExclusionZK       	exclusionZK;
            
            ringTree = dhtMetaUpdate.getRingTree();
            Log.warning(String.format("RingMaster.dhtMetaUpdate received: %s\t%s\t%d\t%s\n",
                    dhtMetaUpdate.getNamedRingConfiguration().getRingName(),
                    ringTree.getRingVersionPair(), 
                    ringTree.getRingCreationTime(),
                    new Date(ringTree.getRingCreationTime()).toString()));
            
            // We have received a new ring tree. First resolve dependencies.
            ringConfig = dhtMetaUpdate.getNamedRingConfiguration().getRingConfiguration();
            ringMC =  new com.ms.silverking.cloud.toporing.meta.MetaClient(dhtMetaUpdate.getNamedRingConfiguration(), 
                                                                           zkConfig);
            dhtMC = new com.ms.silverking.cloud.dht.meta.MetaClient(dhtName, zkConfig);
            storagePolicyGroupZK = new StoragePolicyGroupZK(ringMC);
            
            ringID = dhtMetaUpdate.getRingID();

            zkidLimit = Long.MAX_VALUE; // FUTURE - find better source
            zk = ringMC.getZooKeeper();
            storagePolicyGroupVersion = zk.getVersionPriorTo(ringMC.getMetaPaths().getStoragePolicyGroupPath(), zkidLimit);
            storagePolicyGroup = storagePolicyGroupZK.readFromZK(storagePolicyGroupVersion, null);
            
            // Now compute the resolved replica map
            resolvedReplicaMap = ringTree.getResolvedMap(ringConfig.getRingParentName(), replicaPrioritizer);
            
            // Construct map state
            // Altered to not fiddle with the map here
            // we presume that we have a good map from the DependencyWatcher
            // any exclusions are handled in RingMapState
            
            return new RingMapState2(nodeID, dhtMetaUpdate, ringID, 
                                           storagePolicyGroup, ringMC, ExclusionSet.emptyExclusionSet(0)latestExclusionSet, this, dhtMC,
                                           convergenceAlreadyComplete);
            
            // FUTURE - think about below
            // We currently replace a pre-existing map if we receive a new update for an old map. 
            // Receiving this update can happen when we switch back to a ring that we used previously.
            //mapStates.put(ringIDAndVersionPair, newMapState); 
        } catch (IOException ioe) {
            Log.logErrorWarning(ioe);
        } catch (KeeperException ke) {
            Log.logErrorWarning(ke);
        }
        return null;
    }
    
    public void updateCurMapState() {
        if (curMapState != null) {
        	Log.warning("Updating current map state");
            try {
				curMapState.newExclusionSet(latestExclusionSet, replicaPrioritizer);
			} catch (InvalidRingException ire) {
				Log.logErrorWarning(ire);
			}
        }
    }
    */
    
    // Used only when getting a list of replicas for the __Replicas__ namespace.
    // For now, only return the current replicas. Think about adding the target.
    public IPAndPort[] getReplicas(DHTKey key, OwnerQueryMode ownerQueryMode) {
        return curMapState.getResolvedReplicaMap().getReplicas(key, ownerQueryMode);
    }
    
    public List<RingEntry> getCurrentMapReplicaEntries(IPAndPort replica, OwnerQueryMode oqm) {
        return curMapState.getResolvedReplicaMap().getReplicaEntries(replica, oqm);
    }
    
    public List<RingEntry> getTargetMapReplicaEntries(IPAndPort replica, OwnerQueryMode oqm) {
        return targetMapState.getResolvedReplicaMap().getReplicaEntries(replica, oqm);
    }
    
    public double getCurrentOwnedFraction(IPAndPort replica, OwnerQueryMode oqm) {
    	return RingRegion.getTotalFraction(RingEntry.getRegions(getCurrentMapReplicaEntries(replica, oqm)));
    }
    
    /**
     * All replicas in the current map
     * @return
     */
    public Set<IPAndPort> getAllCurrentReplicaServers() {
        return curMapState.getResolvedReplicaMap().allReplicas();
    }
    
    /**
     * All replicas in the current and target maps
     * @return
     */
    public Set<IPAndPort> getAllCurrentAndTargetReplicaServers() {
    	ImmutableSet.Builder<IPAndPort>	allReplicas;
    	
    	allReplicas = ImmutableSet.builder();
    	allReplicas.addAll(curMapState.getResolvedReplicaMap().allReplicas());
    	if (targetMapState != null) {
    		allReplicas.addAll(targetMapState.getResolvedReplicaMap().allReplicas());
    	}
    	return allReplicas.build();
    }
    
    public Set<IPAndPort> getAllCurrentAndTargetNonExcludedNonLocalReplicaServers() {
        Set<IPAndPort>  allReplicas;
        ExclusionSet    curExclusionSet;
        Set<IPAndPort>  nonExcludedReplicas;
        
    	allReplicas = getAllCurrentAndTargetReplicaServers();
        curExclusionSet = latestExclusionSet;
        nonExcludedReplicas = new HashSet<>(curExclusionSet.filterByIP(allReplicas));
        nonExcludedReplicas.remove(nodeID);
        return ImmutableSet.copyOf(nonExcludedReplicas);
    }
    
    // used for namespace replica requests
    public Set<IPAndPort> getAllCurrentNonExcludedNonLocalReplicaServers() {
        Set<IPAndPort>  replicas;
        
        replicas = new HashSet<>(getAllCurrentNonExcludedReplicaServers());
        replicas.remove(nodeID);
        return ImmutableSet.copyOf(replicas);
    }
    
    private Set<IPAndPort> getAllCurrentNonExcludedReplicaServers() {
        Set<IPAndPort>  nonExcludedReplicas;
        Set<IPAndPort>  allReplicas;
        ExclusionSet    curExclusionSet;
        
        allReplicas = curMapState.getResolvedReplicaMap().allReplicas();
        //curExclusionSet = curMapState.getCurrentExclusionSet();
        curExclusionSet = latestExclusionSet;
        nonExcludedReplicas = ImmutableSet.copyOf(curExclusionSet.filterByIP(allReplicas));
        return nonExcludedReplicas;
    }
    
    /*
    // Used only by ChecksumTreeServer. For now, just use the current map.
    public Collection<RingRegion> getCurrentRegions() {
        return curMapState.getResolvedReplicaMap().getRegions();
    }
    
    public RingIDAndVersionPair getCurrentRingIDAndVersion() {
        return curMapState.getRingIDAndVersionPair();
    }
    
    public long getCurrentDHTConfigVersion() {
        return curMapState.getDHTConfigVersion();
    }
    
    public ConvergencePoint getCurrentConvergencePoint() {
        return curMapState.getConvergencePoint();
    }
    
    public ExclusionSet getCurrentExclusionSet() {
        return curMapState.getCurrentExclusionSet();
    }
    */
    
    private RingMapState2 getMapState(RingIDAndVersionPair ringIDAndVersion) {
        RingMapState2    mapState;
        
        //mapState = mapStates.get(ringIDAndVersion);
        if (curMapState != null && curMapState.getConvergencePoint().getRingIDAndVersionPair().equals(ringIDAndVersion)) {
        	mapState = curMapState;
        } else if (targetMapState != null && targetMapState.getConvergencePoint().getRingIDAndVersionPair().equals(ringIDAndVersion)) {
        	mapState = targetMapState;
        } else {
        	mapState = null;
        }
        return mapState;
    }
    
    /**
     * All regions in the current map
     * @return
     */
    public List<RingRegion> getAllCurrentRegions() {
        return curMapState.getResolvedReplicaMap().getRegions();
    }
    
    public Collection<RingRegion> getRegions(RingIDAndVersionPair ringIDAndVersion) {
        RingMapState2    mapState;
        
        mapState = getMapState(ringIDAndVersion);
        if (mapState != null) {
            return mapState.getResolvedReplicaMap().getRegions();
        } else {
            if (debug) {
                System.out.println("No MapState for: "+ ringIDAndVersion);
            }
            return null;
        }
    }

    // sorts putting suspects at last 
    private IPAndPort[] _getReplicasSorted(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType opType) {
    	Set<IPAndPort>	replicas;
    	IPAndPort[]		sorted;
    	int				goodIndex;
    	int				suspectIndex;
    	
    	replicas = _getReplicas(key, oqm, opType);
    	sorted = new IPAndPort[replicas.size()];
    	goodIndex = 0;
    	suspectIndex = sorted.length - 1;
    	for (IPAndPort replica : replicas) {
    		if (!peerHealthMonitor.isSuspect(replica)) {
    			sorted[goodIndex] = replica;
    			++goodIndex;
    		} else {
    			sorted[suspectIndex] = replica;
    			--suspectIndex;
    		}
    	}
    	assert goodIndex == suspectIndex + 1;
    	return sorted;
    }
    
    public boolean iAmPotentialReplicaFor(DHTKey key) {
    	return isPotentialReplicaFor(key, nodeID);
    }
    
    private boolean isPotentialReplicaFor(DHTKey key, IPAndPort replica) {
        RingMapState2    _targetMapState;
        
        _targetMapState = targetMapState;
        if (_targetMapState != null && _targetMapState != curMapState) {
        	if (_targetMapState.getResolvedReplicaMap().getReplicaSet(key, OwnerQueryMode.All).contains(replica)) {
        		return true;
        	}
        }
    	return curMapState.getResolvedReplicaMap().getReplicaSet(key, OwnerQueryMode.All).contains(replica);
    }
    
    private Set<IPAndPort> _getReplicas(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType opType) {
        RingMapState2    _targetMapState;
        
        _targetMapState = targetMapState;
        if (_targetMapState != null && _targetMapState != curMapState) {
            TransitionReplicaSources    replicaSources;
            Set<IPAndPort>              replicas;
            
            replicaSources = _targetMapState.getReplicaSources(opType);
            switch (replicaSources) {
            case OLD: 
                replicas = curMapState.getResolvedReplicaMap().getReplicaSet(key, oqm);
                break;
            case NEW:
                replicas = _targetMapState.getResolvedReplicaMap().getReplicaSet(key, oqm);
                break;
            case OLD_AND_NEW:
                replicas = union(curMapState.getResolvedReplicaMap().getReplicaSet(key, oqm),
                        _targetMapState.getResolvedReplicaMap().getReplicaSet(key, oqm));
                break;
            default: throw new RuntimeException("panic");
            }
            // FUTURE - think about the order here...
            return replicas;
        } else {
            return curMapState.getResolvedReplicaMap().getReplicaSet(key, oqm);
        }
    }
    
    public IPAndPort[] getReplicas(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType opType) {
        RingMapState2    _targetMapState;
        
        _targetMapState = targetMapState;
        if (_targetMapState != null && _targetMapState != curMapState) {
            return _getReplicasSorted(key, oqm, opType);
        } else {
            return curMapState.getResolvedReplicaMap().getReplicas(key, oqm);
        }
    }
    
    public List<IPAndPort> getReplicaList(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType opType) {
        RingMapState2    _targetMapState;
        
        _targetMapState = targetMapState;
        if (_targetMapState != null && _targetMapState != curMapState) {
            return ImmutableList.copyOf(_getReplicasSorted(key, oqm, opType));
        } else {
            return curMapState.getResolvedReplicaMap().getReplicaList(key, oqm);
        }
    }
    
    // currently unused
    public Set<IPAndPort> getReplicaSet(DHTKey key, OwnerQueryMode oqm, RingOwnerQueryOpType opType) {
        return _getReplicas(key, oqm, opType);
    }
    
    public PrimarySecondaryIPListPair getReplicaListPair(DHTKey key, RingOwnerQueryOpType opType) {
        switch (opType) {
        case Write:
            RingMapState2    _targetMapState;
            
            _targetMapState = targetMapState;
            if (_targetMapState != null && _targetMapState != curMapState) {
                return PrimarySecondaryIPListPair.merge(curMapState.getResolvedReplicaMap().getReplicaListPair(key),
                        _targetMapState.getResolvedReplicaMap().getReplicaListPair(key));
            }
            // else we fall through
        case Read:
            return curMapState.getResolvedReplicaMap().getReplicaListPair(key);
        default: throw new RuntimeException("panic");
        }
    }
    
    public Set<RingEntry> getAllCurrentRingEntries() {
    	return curMapState.getResolvedReplicaMap().getEntries();
    }
    
    public List<RingEntry> getEntries(RingRegion region) {
        return curMapState.getResolvedReplicaMap().getEntries(region);
    }
    
    public Set<IPAndPort> getReplicas(RingRegion region, OwnerQueryMode oqm) {
        return curMapState.getResolvedReplicaMap().getOwners(region, oqm);
    }
    
    private Set<IPAndPort> union(Set<IPAndPort> r1, Set<IPAndPort> r2) {
        Set<IPAndPort> r3;
        
        r3 = new HashSet<>(r1.size() + r2.size());
        r3.addAll(r1);
        r3.addAll(r2);
        return r3;
    }
    
    public Set<IPAndPort> getSecondarySet(Set<SecondaryTarget> secondaryTargets) {
        return curMapState.getSecondarySet(secondaryTargets);
    }
    
    ///////////////////////////////////////////////////////////////////////////
    
    private String getRingNameFromCP(ConvergencePoint cp) throws KeeperException {
		Triple<String,Long,Long>	t;
		DHTRingCurTargetZK			ctZK;
		
		ctZK = new DHTRingCurTargetZK(dhtMetaReader.getMetaClient(), dhtMetaReader.getDHTConfig()); 
		t = ctZK.getCurRingAndVersionPair();
		if (!t.getTail().equals(cp.getRingIDAndVersionPair().getRingVersionPair())) {
			t = ctZK.getTargetRingAndVersionPair();
			if (!t.getTail().equals(cp.getRingIDAndVersionPair().getRingVersionPair())) {
				Log.warning(t.getTail());
				Log.warning(cp.getRingIDAndVersionPair().getRingVersionPair());
				throw new RuntimeException("cp version mismatch");
			} else {
				return t.getV1();
			}
		} else {		
			return t.getV1();
		}
    }
    
	private RingMapState2 newMapState(ConvergencePoint cp) throws KeeperException, IOException {
		DHTMetaUpdate	dhtMetaUpdate;
		String			ringName;
        com.ms.silverking.cloud.toporing.meta.MetaClient ringMC;

        ringName = getRingNameFromCP(cp);
		dhtMetaUpdate = dhtMetaReader.readRing(ringName, cp.getRingIDAndVersionPair().getRingVersionPair());
        ringMC =  new com.ms.silverking.cloud.toporing.meta.MetaClient(dhtMetaUpdate.getNamedRingConfiguration(), 
                zkConfig);
		
		return new RingMapState2(nodeID, dhtMetaUpdate, cp.getRingIDAndVersionPair().getRingID(), null, 
								 ringMC, ExclusionSet.emptyExclusionSet(0), dhtMetaReader.getMetaClient());
	}
	
    private static final int	zkReadAttempts = 90;
    private static final int	zkReadRetryIntervalMillis = 1 * 1000;	
	
	public void initializeMap(DHTConfiguration dhtConfig) throws IOException, KeeperException {
		Triple<String,Long,Long>	curRingAndVersionPair;
		ConvergencePoint			currentCP;
		long						creationTime;
		int							attemptIndex;
		String						ringName;
		Pair<Long,Long>				configAndInstance;

		curRingAndVersionPair = new DHTRingCurTargetZK(dhtMetaReader.getMetaClient(), dhtMetaReader.getDHTConfig()).getCurRingAndVersionPair();
		ringName = curRingAndVersionPair.getHead();
		configAndInstance = curRingAndVersionPair.getTail();
		
        creationTime = Long.MIN_VALUE;
        attemptIndex = 0;
        do {
        	String	instanceBase;
        	
        	instanceBase = MetaPaths.getRingConfigInstancePath(ringName, configAndInstance.getV1());        	
        	try {
				creationTime = dhtMetaReader.getMetaClient().getZooKeeper().getCreationTime(ZooKeeperExtended.padVersionPath(instanceBase, configAndInstance.getV2()));
			} catch (KeeperException e) {
				Log.logErrorWarning(e);
			}
        	if (creationTime < 0) {
				ThreadUtil.sleep(zkReadRetryIntervalMillis);
        	}
        	++attemptIndex;
        } while (creationTime < 0 && attemptIndex < zkReadAttempts);
        if (creationTime < 0) {
        	throw new RuntimeException("Unable to initialize map: "+ curRingAndVersionPair);
        }
		
		currentCP = new ConvergencePoint(dhtConfig.getVersion(), RingIDAndVersionPair.fromRingNameAndVersionPair(curRingAndVersionPair), creationTime);
		setCurMapState(newMapState(currentCP));
	}
	
	private void setCurMapState(RingMapState2 _curMapState) {
		Log.warningf("curMapState was %s", curMapState != null ? curMapState.getConvergencePoint() : "null");
		curMapState = _curMapState;
		Log.warningf("setCurMapState %s", curMapState != null ? curMapState.getConvergencePoint() : "null");
	}

	private void setTargetMapState(RingMapState2 _targetMapState) {
		Log.warningf("targetMapState was %s", targetMapState != null ? targetMapState.getConvergencePoint() : "null");
		targetMapState = _targetMapState;
		Log.warningf("setTargetMapState %s", targetMapState != null ? targetMapState.getConvergencePoint() : "null");
	}
	
	public void setConvergenceState(MessageGroup message, MessageGroupConnection connection) {
		RingState		state;
		ProtoOpResponseMessageGroup	response;
		OpResult		result;
		ConvergencePoint	curCP;
		ConvergencePoint	targetCP;
		
		state = ProtoSetConvergenceStateMessageGroup.getRingState(message);
		curCP = ProtoSetConvergenceStateMessageGroup.getCurCP(message);
		targetCP = ProtoSetConvergenceStateMessageGroup.getTargetCP(message);
		Log.warningf("setConvergenceState %s %s %s", state, curCP, targetCP);
		
		if (!curMapState.getConvergencePoint().equals(curCP)) {
			Log.warningf("!curMapState.getConvergencePoint().equals(curCP)");
			// This is valid if the node was not in the last ring
			// in which case, nobody should be trying to read from this replica
			//result = OpResult.ERROR;
		}
		//} else {		
			if (targetMapState == null || (!targetMapState.getConvergencePoint().equals(targetCP) && state != RingState.ABANDONED)) {
				if (state == RingState.INITIAL) {
					try {
						setTargetMapState(newMapState(targetCP));
						result = OpResult.SUCCEEDED;
					} catch (Exception e) {
						Log.logErrorWarning(e);
						result = OpResult.ERROR;
					}
				} else {
					Log.warningf("targetMapState error. state %s", state);
					if (targetMapState == null) {
						Log.warningf("targetMapState == null");
					} else {
						Log.warningf("!targetMapState.getConvergencePoint().equals(targetCP) %s", !targetMapState.getConvergencePoint().equals(targetCP));
					}
					result = OpResult.ERROR;
				}
			} else {
				result = targetMapState.setState(state);
				if (state == RingState.CLOSED && result == OpResult.SUCCEEDED) {
					setCurMapState(targetMapState);
					setTargetMapState(null);
				} else if (state == RingState.ABANDONED) {
					setTargetMapState(null);
				}
			}
		//}
		
		Log.warningf("setConvergenceState result %s", result);
		response = new ProtoOpResponseMessageGroup(message.getUUID(), 0, result, myOriginatorID.getBytes(), message.getDeadlineRelativeMillis());
		try {
			connection.sendAsynchronous(response.toMessageGroup(), SystemTimeUtil.systemTimeSource.absTimeMillis() + message.getDeadlineRelativeMillis());
		} catch (IOException ioe) {
			Log.logErrorWarning(ioe);
		}
	}
}

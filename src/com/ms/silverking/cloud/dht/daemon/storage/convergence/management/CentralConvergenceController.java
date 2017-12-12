package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingState;
import com.ms.silverking.cloud.dht.meta.DHTMetaReader;
import com.ms.silverking.cloud.dht.meta.DHTMetaUpdate;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.ProtoSetConvergenceStateMessageGroup;
import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.HostGroupTableZK;
import com.ms.silverking.cloud.ring.IntersectionResult;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.RingEntry;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

public class CentralConvergenceController extends ConvergenceControllerBase implements RequestController {
	private final String			curRingName;
	private final DHTMetaUpdate		curRing;
	private final ConvergencePoint	curCP;
	private RingState				ringState;
	private final ResolvedReplicaMap	curMap;
	private final RingConfiguration	currentRingConfig;
	private final boolean			syncUnchangedOwners;
	private final SyncMode			mode;
	
	private static final boolean	serializeNamespaces = false;
	
	private static final int	initialStateAttempts = 1;
	private static final int	abandonedStateAttempts = 3;
	
	public enum SyncTargets {
		Primary, Secondary, All;
		
		public OwnerQueryMode getOwnerQueryMode() {
			switch (this) {
			case Primary: return OwnerQueryMode.Primary;
			case Secondary: return OwnerQueryMode.Secondary;
			case All: return OwnerQueryMode.All;
			default: throw new RuntimeException("Panic");
			}
		}
	}
	
	public enum RequestedSyncMode	{
		SyncAndSetState, SyncAndSetStateUnlessSubset, SetStateOnly, SyncOnly;

		public SyncMode getSyncMode() {
			switch (this) {
			case SyncAndSetState: return SyncMode.SyncAndSetState;
			case SyncAndSetStateUnlessSubset: throw new RuntimeException("Can't get SyncMode for SyncAndSetStateUnlessSubset");
			case SetStateOnly: return SyncMode.SetStateOnly;
			case SyncOnly: return SyncMode.SyncOnly;
			default: throw new RuntimeException("panic");
			}
		}
	};
	
	private enum SyncMode {
		SyncAndSetState, SetStateOnly, SyncOnly;
		
		public boolean setsState() {
			switch (this) {
			case SyncAndSetState: return true;
			case SetStateOnly: return true;
			case SyncOnly: return false;
			default: throw new RuntimeException("panic");
			}
		}
		
		public boolean doesSync() {
			switch (this) {
			case SyncAndSetState: return true;
			case SetStateOnly: return false;
			case SyncOnly: return true;
			default: throw new RuntimeException("panic");
			}
		}
	}
	
	private static final boolean	verbose = true || ConvergenceControllerBase.verbose;
	private static final boolean	debug = true || ConvergenceControllerBase.debug;
	
	/*
	 * As a first cut, ignore stopping ongoing
	 * Also, stay agnostic as to whether this replaces ring master or is called by ring master
	 * For now, concentrate on a single convergence
	 */
	
	/*
	 * Required functions:
	 * set state on nodes...
	 * get list of nodes...
	 */
	
	/*
	 * Steps:
	 *  state->
	 * 
	 *  get new ring
	 *  get all namespaces
	 *  for each namespace
	 *    for each region in the new ring
	 *      for each replica in the new region
	 *         sync the region
	 * 
	 *  state->complete
	 */
	
	public CentralConvergenceController(UUIDBase uuid, DHTMetaReader dhtMetaReader, ConvergencePoint curCP, ConvergencePoint targetCP, 
										ExclusionSet exclusionSet, MessageGroupBase	mgBase, boolean syncUnchangedOwners,
										RequestedSyncMode requestedSyncMode) throws KeeperException, IOException {
		super(uuid, dhtMetaReader, targetCP, exclusionSet, mgBase);
		assert curCP != null;
		assert curCP.getRingIDAndVersionPair() != null;
		this.curCP = curCP;
		this.syncUnchangedOwners = syncUnchangedOwners;
		curRingName = getRingNameFromCP(curCP);
		curRing = dhtMetaReader.readRing(curRingName, curCP.getRingIDAndVersionPair().getRingVersionPair());
		
		Log.warningAsyncf("Using exclusions:\n%s", exclusionSet);
		
		syncController = new SyncController(mgBase, curCP, targetCP, SystemTimeUtil.systemTimeSource);
		
		RingConfigurationZK	ringConfigZK;
		
		ringConfigZK = new RingConfigurationZK(ringMC);
		Log.warningAsync("Reading ring configuration: ", curRing.getRingIDAndVersionPair());
		currentRingConfig = ringConfigZK.readFromZK(curRing.getRingIDAndVersionPair().getRingVersionPair().getV1(), null);
		Log.warningAsync("Current ring configuration: ", currentRingConfig);

		curMap = getResolvedReplicaMap(curRing, currentRingConfig);
		
		if (requestedSyncMode == RequestedSyncMode.SyncAndSetStateUnlessSubset) {
			if (curMap.isSubset(targetReplicaMap)) {
				mode = SyncMode.SetStateOnly;
			} else {
				mode = SyncMode.SyncAndSetState;
			}
		} else {
			mode = requestedSyncMode.getSyncMode();
		}
		
		/*
		Log.warningAsync("*****************************");
		Log.warningAsync("Current map");
		curMap.display();
		Log.warningAsync("*****************************");
		*/		
	}
	
    //////////////////////////////////////////////////////////////////
    
    private static final int	setStateDeadlineRelativeMillis = 6 * 60 * 1000;
    
    // FUTURE - Some code is replicated in SKAdmin. Deduplicate this code. 
    
	private static HostGroupTable getHostGroupTable(String hostGroupTableName, ZooKeeperConfig zkConfig) throws KeeperException, IOException {
		HostGroupTableZK	hostGroupTableZK;
		com.ms.silverking.cloud.meta.MetaClient	cloudMC;
		
		cloudMC = new com.ms.silverking.cloud.meta.MetaClient(CloudConfiguration.emptyTemplate.hostGroupTableName(hostGroupTableName), zkConfig);
		hostGroupTableZK = new HostGroupTableZK(cloudMC);		
		return hostGroupTableZK.readFromZK(-1, null);
	}
	
	private Set<IPAndPort> findValidPassiveServers(Set<String> passiveNodeHostGroupNames, HostGroupTable hostGroupTable) {
		ImmutableSet.Builder<String>	validServers;
		
		validServers = ImmutableSet.builder();
		for (String hostGroupName : passiveNodeHostGroupNames) {
			validServers.addAll(hostGroupTable.getHostAddresses(hostGroupName));
		}
		return IPAndPort.set(validServers.build(), dhtConfig.getPort());
	}    
	
	protected Set<IPAndPort> getCurrentReplicasWithPorts() {
		return getReplicasWithPorts(curMap);
	}
    
    private Set<IPAndPort> getPassiveNodes() throws ConvergenceException {
    	try {
			HostGroupTable			hostGroupTable;
			Set<IPAndPort>			validPassiveServers;
			String					hostGroupTableName;
			Set<String>				passiveNodeHostGroupNames;
			Set<IPAndPort>			currentReplicas;
			Set<IPAndPort>			targetReplicas;
			Set<IPAndPort>			currentButNonTargetReplicas;
			
			hostGroupTableName = currentRingConfig.getCloudConfiguration().getHostGroupTableName();
			Log.warningAsync("hostGroupTableName: ", hostGroupTableName);
			hostGroupTable = getHostGroupTable(hostGroupTableName, ringMC.getZooKeeper().getZKConfig());
			
			// FUTURE - Do more validation of configuration. E.g. prevent a server from being both
			// active and passive, the ring from containing servers without class vars, etc.
			
			passiveNodeHostGroupNames = dhtConfig.getPassiveNodeHostGroupsAsSet();
			Log.warningAsync("passiveNodeHostGroupNames: ", CollectionUtil.toString(passiveNodeHostGroupNames));
			
			currentReplicas = getCurrentReplicasWithPorts();
			targetReplicas = getTargetReplicasWithPorts();
			currentButNonTargetReplicas = new HashSet<>(currentReplicas);
			currentButNonTargetReplicas.removeAll(targetReplicas);
			
			validPassiveServers = new HashSet<>(findValidPassiveServers(passiveNodeHostGroupNames, hostGroupTable));
			validPassiveServers.addAll(currentButNonTargetReplicas);
			validPassiveServers.removeAll(IPAndPort.set(exclusionSet.getServers(), dhtConfig.getPort()));
			validPassiveServers.removeAll(targetReplicas);
			Log.warningAsync("validPassiveServers: ", CollectionUtil.toString(validPassiveServers));
			
			return validPassiveServers;
    	} catch (IOException | KeeperException e) {
    		throw new ConvergenceException("Exception reading passive nodes", e);
    	}
    }
    
    private void displayErrors(Map<UUIDBase,IPAndPort> replicaMap, Set<UUIDBase> failed, String label) {
    	for (UUIDBase uuid : failed) {
    		IPAndPort	replica;
    		
    		replica = replicaMap.get(uuid);
    		Log.warningAsyncf("%s: %s", label, replica);
    	}
    }
    
    public void setNodesState(RingState ringState, int maxAttempts) throws ConvergenceException {
    	int		attempt;
    	boolean	successful;
    	
    	if (maxAttempts < 1) {
    		throw new RuntimeException("maxAttempts < 1: "+ maxAttempts);
    	}
    	attempt = 1;
    	successful = false;
    	while (!successful && attempt <= maxAttempts) {
    		try {
    			setNodesState(ringState);
    			successful = true;
    		} catch (ConvergenceException ce) {
    			if (attempt >= maxAttempts) {
    				throw ce;
    			}
    		}
    		++attempt;
    	}
    }
    
    public void setNodesState(RingState ringState) throws ConvergenceException {
    	Set<UUIDBase>	msgUUIDs;
    	Set<UUIDBase>	incompleteUUIDs;
    	Set<UUIDBase>	failedUUIDs;
    	Pair<Set<UUIDBase>,Set<UUIDBase>>	result;
    	Map<UUIDBase,IPAndPort>	replicaMap;
    	Set<IPAndPort>	targetReplicas;
    	Set<IPAndPort>	passiveNodes;
    	
    	Log.warningAsyncf("setNodesState %s %s", targetRing.getRingIDAndVersionPair(), ringState);
    	replicaMap = new HashMap<>();
    	msgUUIDs = new HashSet<>();
    	
    	// Also, send to source? but don't wait? unless source is now passive?
    	// fix passive to include unused?
    	
    	targetReplicas = getTargetReplicasWithPorts();
    	passiveNodes = getPassiveNodes();
    	
    	for (IPAndPort replica : targetReplicas) {
            UUIDBase		uuid;
        	
            uuid = new UUIDBase(false);
            opUUIDs.add(uuid);
    		uuid = sendSetState(uuid, replica, ringState);
    		msgUUIDs.add(uuid);
    		replicaMap.put(uuid, replica);
    	}    	
    	for (IPAndPort passiveNode : passiveNodes) {
    		UUIDBase	uuid;
    		
            uuid = new UUIDBase(false);
            opUUIDs.add(uuid);
    		uuid = sendSetState(uuid, passiveNode, ringState);
    		msgUUIDs.add(uuid);
    		replicaMap.put(uuid, passiveNode);
    	}
    	
    	result = opCompletionTracker.waitForCompletion(msgUUIDs, setStateDeadlineRelativeMillis, TimeUnit.MILLISECONDS);
    	opUUIDs.removeAll(msgUUIDs);
    	incompleteUUIDs = result.getV1();
    	failedUUIDs = result.getV2();
    	displayErrors(replicaMap, incompleteUUIDs, "Incomplete");
    	displayErrors(replicaMap, failedUUIDs, "Failed");
    	if (incompleteUUIDs.size() != 0) {
    		throw new ConvergenceException("Incomplete ops: "+ incompleteUUIDs.size());
    	}
    	if (failedUUIDs.size() != 0) {
    		throw new ConvergenceException("Failed ops: "+ failedUUIDs.size());
    	}
    	this.ringState = ringState;
    	Log.warningAsyncf("setNodesState complete %s %s", targetRing.getRingIDAndVersionPair(), ringState);    	
    }

    private UUIDBase sendSetState(UUIDBase uuid, IPAndPort replica, RingState ringState) {
        MessageGroup    mg;
        mg = new ProtoSetConvergenceStateMessageGroup(uuid, mgBase.getMyID(), setStateDeadlineRelativeMillis, curCP, targetCP, ringState).toMessageGroup();
        mgBase.send(mg, replica);
        return uuid;
    }
        
    //////////////////////////////////////////////////////////////////
    
    private void syncRegion(long ns, RingEntry targetEntry, SyncTargets syncTargets, Action upstreamDependency, List<ReplicaSyncRequest> srList) throws ConvergenceException {
        List<RingEntry> sourceEntries;
        
    	//Log.warningAsyncf("syncRegion %x %s", ns, targetEntry);
        sourceEntries = curMap.getEntries(targetEntry.getRegion());
        if (!sourceEntries.isEmpty()) {
            //Log.warningAsyncf("%x target %s\towners %s\n", ns, targetEntry.getRegion(), CollectionUtil.toString(sourceEntries));
            for (RingEntry sourceEntry : sourceEntries) {
                List<IPAndPort> sourceOwners;
                List<IPAndPort> nonExcludedSourceOwners;
                
                sourceOwners = new ArrayList<>(sourceEntry.getOwnersIPList(OwnerQueryMode.Primary));
                
                Log.info("Filtering exclusion set: ", exclusionSet);
                nonExcludedSourceOwners = exclusionSet.filterByIP(sourceOwners);
                if (nonExcludedSourceOwners.size() != sourceOwners.size()) {
                    Log.warningAsync("Raw sourceOwners:      ", sourceOwners);
                    Log.warningAsync("Filtered nonExcludedSourceOwners: ", nonExcludedSourceOwners);
                }
                
                if (nonExcludedSourceOwners.size() == 0) {
                	Log.warningAsyncf("%x All nonLocalOwners excluded. Ignoring exclusions for this entry.", ns);
                } else {
                	sourceOwners = nonExcludedSourceOwners;
                }
                
            	IntersectionResult	iResult;
            	
            	// We don't want to request the entire source region.
            	// We're only interested in the portion(s) of the source region that cover(s) the target region.
            	//Log.warningAsyncf("Intersection %s %s", sourceEntry.getRegion(), targetEntry.getRegion());
            	iResult = RingRegion.intersect(sourceEntry.getRegion(), targetEntry.getRegion());
            	for (RingRegion commonSubRegion : iResult.getOverlapping()) {
                    List<IPAndPort> targetOwners;
                    List<IPAndPort> nonExcludedTargetOwners;
                    
                    targetOwners = new ArrayList<>(targetEntry.getOwnersIPList(syncTargets.getOwnerQueryMode()));
                    nonExcludedTargetOwners = exclusionSet.filterByIP(targetOwners);
                    
            		for (IPAndPort newOwner : nonExcludedTargetOwners) {
            			if (sourceOwners.contains(newOwner) && !syncUnchangedOwners) {
                    		Log.warningAsyncf("Skipping unchanged owner: %s", newOwner);
            			} else {
            				Action	prev;
            				
            				prev = upstreamDependency;
		            		for (IPAndPort sourceOwner : nonExcludedSourceOwners) {
                        		prev = syncReplica(ns, commonSubRegion, newOwner.port(dhtConfig.getPort()), sourceOwner.port(dhtConfig.getPort()), prev, srList);
		            		}
            			}
            		}
            	}
            }
        } else {
        	// This should actually never occur as the getEntries() call above
        	// has no notion of local/non-local. It just returns the owners, and there 
        	// should always be owners.
            Log.warningAsyncf("Primary convergence %x. No previous non-local owners for entry: ", ns, targetEntry);
            throw new ConvergenceException("Unexpected no previous non-local owners for entry");
        }
    	//Log.warningAsyncf("Done syncRegion %x %s", ns, targetEntry);
    }
    
    private Action syncNamespace(long ns, SyncTargets syncTargets, Action upstreamDependency) throws ConvergenceException {
    	Set<RingEntry>			entries;
    	SynchronizationPoint	syncPoint;
    	List<ReplicaSyncRequest> srList;
    	
    	Log.warningAsyncf("Synchronizing %x", ns);
    	srList = new ArrayList<>();
    	entries = targetReplicaMap.getEntries();
    	for (RingEntry entry : entries) {
    		syncRegion(ns, entry, syncTargets, upstreamDependency, srList);
    	}
    	Log.warningAsyncf("Done synchronizing %x", ns);
    	
    	syncPoint = SynchronizationPoint.of(Long.toHexString(ns), srList.toArray(new Action[0]));
    	// Note: downstream computed from upstream later
    	if (serializeNamespaces) {
    		syncController.addAction(syncPoint);
    	} else {
    		syncController.addCompleteAction(syncPoint);
    	}
    	return syncPoint;
    }
    
    public void syncAll(Set<Long> namespaces, SyncTargets syncTargets) throws ConvergenceException {
    	Action prevDependency;
    	
    	Log.warningAsync("Synchronizing namespaces");
    	prevDependency = null;
    	for (long ns : namespaces) {
    		prevDependency = syncNamespace(ns, syncTargets, prevDependency);
    	}
    	Log.warningAsync("Done synchronizing namespaces");
    	
    	syncController.freeze();
    	
    	Log.warningAsync(" *** Sending requests");
    	syncController.waitForCompletion(1, TimeUnit.DAYS); // FUTURE - improve this from a failsafe to a real limit
    	Log.warningAsync(" *** Requests complete");
    }
    
    public void converge(SyncTargets syncTargets) throws ConvergenceException {
    	boolean	succeeded;
    	
    	succeeded = false;
    	try {
	    	Set<Long>	namespaces;
	    	
	    	Log.warningAsync("Starting convergence ", targetRing.getRingIDAndVersionPair());
	    	if (mode.setsState()) {
	    		setNodesState(RingState.INITIAL, initialStateAttempts);
	    	}
	    	namespaces = getAllNamespaces();
	    	// need to send all namespaces out to the nodes
	    	if (mode.setsState()) {
	    		setNodesState(RingState.READY_FOR_CONVERGENCE_1);
		    	// sleep here to allow in flight operations to complete on the old replica sets
		    	setNodesState(RingState.READY_FOR_CONVERGENCE_2);
	    	}
	    	
	    	if (mode.doesSync()) {
	    		syncAll(namespaces, syncTargets);
	    	} else {
	    		Log.warningAsync("Skipping syncAll(");
	    	}
	    	
	    	if (mode.setsState()) {
		    	setNodesState(RingState.LOCAL_CONVERGENCE_COMPLETE_1);
		    	setNodesState(RingState.ALL_CONVERGENCE_COMPLETE_1);
		    	setNodesState(RingState.ALL_CONVERGENCE_COMPLETE_2);
		    	setNodesState(RingState.CLOSED);
	    	}
	    	Log.warningAsync("Convergence complete", targetRing.getRingIDAndVersionPair());
	    	succeeded = true;
    	} catch (ConvergenceException ce) {
    		boolean	abandonSetAll;
    		
    		Log.logErrorWarning(ce, "Convergence failed"+ targetRing.getRingIDAndVersionPair());
    		try {
    	    	if (mode.setsState()) {
    	    		setNodesState(RingState.ABANDONED, abandonedStateAttempts);
    	    	}
    			abandonSetAll = true;
    		} catch (ConvergenceException ce2) {
    			abandonSetAll = false;
    		}
    		if (abandonSetAll) {
    			Log.warningAsync("setNodesState(RingState.ABANDONED) succeeded. Previous failure was transient."); 
    		}
    		throw ce;
    	} finally {
    		setComplete(succeeded);
    	}
    }
    
	///////////////////////////////////////////////////
	
	@Override
	public RequestStatus getStatus(UUIDBase uuid) {
		ensureUUIDMatches(uuid);
		if (mode.setsState()) {
			if (ringState != null) {
				switch (ringState) {
				case READY_FOR_CONVERGENCE_2:
					return new SimpleRequestStatus(getRequestState(), String.format("%s:%.0f:%s", ringState.toString(), syncController.elapsedSeconds(), syncController.getStatus()));
				default:
					return new SimpleRequestStatus(getRequestState(), ringState.toString());
				}
			} else {
				return new SimpleRequestStatus(getRequestState(), "<init>");
			}
		} else {
			return new SimpleRequestStatus(getRequestState(), syncController.getStatus().toString());
		}
	}
}

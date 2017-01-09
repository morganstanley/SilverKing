package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.config.HostGroupTable;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.ReplicaNaiveIPPrioritizer;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumTreeRequest;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingState;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.management.OpCompletionTracker;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTMetaReader;
import com.ms.silverking.cloud.dht.meta.DHTMetaUpdate;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoNamespaceResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoOpResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoSetConvergenceStateMessageGroup;
import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.meta.HostGroupTableZK;
import com.ms.silverking.cloud.ring.IntersectionResult;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.toporing.InvalidRingException;
import com.ms.silverking.cloud.toporing.ResolvedReplicaMap;
import com.ms.silverking.cloud.toporing.RingEntry;
import com.ms.silverking.cloud.toporing.RingTree;
import com.ms.silverking.cloud.toporing.RingTreeBuilder;
import com.ms.silverking.cloud.toporing.meta.NamedRingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfiguration;
import com.ms.silverking.cloud.toporing.meta.RingConfigurationZK;
import com.ms.silverking.cloud.zookeeper.ZooKeeperConfig;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

public class CentralConvergenceController implements RequestController {
	private final UUIDBase			uuid;
	private final DHTConfiguration	dhtConfig;
	private final String			curRingName;
	private final String			targetRingName;
	private final DHTMetaUpdate		curRing;
	private final DHTMetaUpdate		targetRing;
	private final ConvergencePoint	curCP;
	private final ConvergencePoint	targetCP;
	private RingState				ringState;
	private final ResolvedReplicaMap	curMap;
	private final ResolvedReplicaMap	targetMap;
	private final DHTMetaReader		dhtMetaReader;	
	private final ExclusionSet		exclusionSet;
	private final MessageGroupBase	mgBase;
	private final ResolvedReplicaMap	targetReplicaMap;
	private final Map<UUIDBase,NamespaceRequest>	nsRequests;
    private final OpCompletionTracker	opCompletionTracker;	
	private final com.ms.silverking.cloud.toporing.meta.MetaClient	ringMC;
	private final RingConfiguration	currentRingConfig;
	private final RingConfiguration	targetRingConfig;
	private final SyncController	syncController;
	private final boolean			syncUnchangedOwners;
	private final Set<UUIDBase>		opUUIDs;
	
    private boolean	abandoned;
    private boolean	complete;
    private final Lock	completionLock;
    private final Condition	completionCV;
	
	private static final boolean	verbose = true;
	private static final boolean	debug = true;
	
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
										ExclusionSet exclusionSet, MessageGroupBase	mgBase, boolean syncUnchangedOwners) throws KeeperException, IOException {	
		assert dhtMetaReader != null;
		assert curCP != null;
		assert targetCP != null;
		assert curCP.getRingIDAndVersionPair() != null;
		this.uuid = uuid;
		dhtConfig = dhtMetaReader.getDHTConfig();
		this.curCP = curCP;
		this.targetCP = targetCP;
		this.dhtMetaReader = dhtMetaReader;
		this.exclusionSet = exclusionSet;
		this.mgBase = mgBase;
		this.syncUnchangedOwners = syncUnchangedOwners;
		curRingName = getRingNameFromCP(curCP);
		targetRingName = getRingNameFromCP(targetCP);
		curRing = dhtMetaReader.readRing(curRingName, curCP.getRingIDAndVersionPair().getRingVersionPair());
		targetRing = dhtMetaReader.readRing(targetRingName, targetCP.getRingIDAndVersionPair().getRingVersionPair());
		
		syncController = new SyncController(mgBase, curCP, targetCP, SystemTimeUtil.systemTimeSource);
		
        NamedRingConfiguration namedRingConfig;
		RingConfigurationZK	ringConfigZK;
		
        namedRingConfig = new NamedRingConfiguration(targetRingName, RingConfiguration.emptyTemplate);        
        ringMC = new com.ms.silverking.cloud.toporing.meta.MetaClient(namedRingConfig, dhtMetaReader.getMetaClient().getZooKeeper().getZKConfig());
		
		ringConfigZK = new RingConfigurationZK(ringMC);
		Log.warningAsync("Reading ring configuration: ", targetRing.getRingIDAndVersionPair());
		currentRingConfig = ringConfigZK.readFromZK(curRing.getRingIDAndVersionPair().getRingVersionPair().getV1(), null);
		Log.warningAsync("Current ring configuration: ", currentRingConfig);
		targetRingConfig = ringConfigZK.readFromZK(targetRing.getRingIDAndVersionPair().getRingVersionPair().getV1(), null);
		Log.warningAsync("Target ring configuration: ", targetRingConfig);

		curMap = getResolvedReplicaMap(curRing, targetRingConfig);
		targetMap = getResolvedReplicaMap(targetRing, targetRingConfig);
		
		Log.warningAsync("*****************************");
		Log.warningAsync("Current map");
		curMap.display();
		Log.warningAsync("*****************************");
		Log.warningAsync("*****************************");
		Log.warningAsync("Target map");
		targetMap.display();
		Log.warningAsync("*****************************");
		
		Log.warningAsync("Reading ring: ", targetRing.getRingIDAndVersionPair());
		targetReplicaMap = targetRing.getRingTree().getResolvedMap(targetRingConfig.getRingParentName(), new ReplicaNaiveIPPrioritizer());
		Log.warningAsync("Reading ring complete: ", targetRing.getRingIDAndVersionPair());
		displayReplicas();
		
		nsRequests = new HashMap<>();	
		opCompletionTracker = new OpCompletionTracker();
		
		completionLock = new ReentrantLock();
		completionCV = completionLock.newCondition();
		
		opUUIDs = new ConcurrentSkipListSet<>();
	}
	
	public UUIDBase getUUID() {
		return uuid;
	}
	
	private ResolvedReplicaMap getResolvedReplicaMap(DHTMetaUpdate metaUpdate, RingConfiguration ringConfig) {
		RingTree	ringTreeMinusExclusions;
		ResolvedReplicaMap	resolvedReplicaMapMinusExclusions;
		
        try {
            ringTreeMinusExclusions = RingTreeBuilder.removeExcludedNodes(metaUpdate.getRingTree(), exclusionSet);
        } catch (InvalidRingException ire) {
            throw new RuntimeException("Unexpected InvalidRingException", ire);
        }
        resolvedReplicaMapMinusExclusions = ringTreeMinusExclusions.getResolvedMap(ringConfig.getRingParentName(), null);
		return resolvedReplicaMapMinusExclusions;
	}
	
	private void displayReplicas() {
		Log.warningAsync("Replicas: ");
		for (IPAndPort replica : targetReplicaMap.allReplicas()) {
			Log.warningAsync(replica);
		}
	}
	
	private Set<IPAndPort> getTargetReplicasWithPorts() {
		Set<IPAndPort>	replicas;
		
		replicas = new HashSet<>(targetReplicaMap.allReplicas().size());
		for (IPAndPort replica : targetReplicaMap.allReplicas()) {
			replicas.add(new IPAndPort(replica.getIP(), dhtConfig.getPort()));
		}
		return replicas;
	}
	
	private String getRingNameFromCP(ConvergencePoint cp) {
		try {			
			return dhtMetaReader.getMetaClient().getDHTConfiguration().getRingName();
		} catch (KeeperException ke) {
			throw new RuntimeException(ke);
		}
	}	
	
    //////////////////////////////////////////////////////////////////
    
    private static final int	setStateDeadlineRelativeMillis = 2 * 60 * 1000;
    
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
    
    private Set<IPAndPort> getPassiveNodes() throws ConvergenceException {
    	try {
			HostGroupTable			hostGroupTable;
			Set<IPAndPort>			validPassiveServers;
			String					hostGroupTableName;
			Set<String>				passiveNodeHostGroupNames;
			
			hostGroupTableName = currentRingConfig.getCloudConfiguration().getHostGroupTableName();
			Log.warningAsync("hostGroupTableName: ", hostGroupTableName);
			hostGroupTable = getHostGroupTable(hostGroupTableName, ringMC.getZooKeeper().getZKConfig());
			
			// FUTURE - Do more validation of configuration. E.g. prevent a server from being both
			// active and passive, the ring from containing servers without class vars, etc.
			
			passiveNodeHostGroupNames = dhtConfig.getPassiveNodeHostGroupsAsSet();
			Log.warningAsync("passiveNodeHostGroupNames: ", CollectionUtil.toString(passiveNodeHostGroupNames));
			
			validPassiveServers = new HashSet<>(findValidPassiveServers(passiveNodeHostGroupNames, hostGroupTable));
			validPassiveServers.removeAll(IPAndPort.set(exclusionSet.getServers(), dhtConfig.getPort()));
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
    
    public void setNodesState(RingState ringState) throws ConvergenceException {
    	Set<UUIDBase>	msgUUIDs;
    	Set<UUIDBase>	incompleteUUIDs;
    	Set<UUIDBase>	failedUUIDs;
    	Pair<Set<UUIDBase>,Set<UUIDBase>>	result;
    	Map<UUIDBase,IPAndPort>	replicaMap;
    	
    	Log.warningAsyncf("setNodesState %s %s", targetRing.getRingIDAndVersionPair(), ringState);
    	replicaMap = new HashMap<>();
    	msgUUIDs = new HashSet<>();
    	for (IPAndPort replica : getTargetReplicasWithPorts()) {
            UUIDBase		uuid;
        	
            uuid = new UUIDBase(false);
            opUUIDs.add(uuid);
    		uuid = sendSetState(uuid, replica, ringState);
    		msgUUIDs.add(uuid);
    		replicaMap.put(uuid, replica);
    	}    	
    	for (IPAndPort passiveNode : getPassiveNodes()) {
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
    
	public void handleOpResponse(MessageGroup message, MessageGroupConnection connection) {
        if (debug) {
            Log.warningAsyncf("Received op response for: %s\tfrom: %s %s", message.getUUID(), connection.getRemoteSocketAddress(), ProtoOpResponseMessageGroup.result(message));
        }
        
        if  (opUUIDs.contains(message.getUUID())) {
        	opCompletionTracker.setComplete(message.getUUID(), ProtoOpResponseMessageGroup.result(message));
        } else {
        	syncController.requestComplete(message.getUUID(), ProtoOpResponseMessageGroup.result(message));
        }
	}        
    
    //////////////////////////////////////////////////////////////////
    
    private static final int	nsRequestTimeoutMillis = 2 * 60 * 1000;
	private NamespaceRequest	nr;
    
    private Set<Long> getAllNamespaces() throws ConvergenceException {
    	boolean				ok;
    	
    	if (nr != null) {
    		throw new RuntimeException("Unexpected reset of NamespaceRequest");
    	}
    	Log.warningAsyncf("Getting all namespaces %s", targetRing.getRingIDAndVersionPair());    	
    	nr = new NamespaceRequest(mgBase, getTargetReplicasWithPorts(), nsRequests);
    	nr.requestNamespacesFromPeers();
    	checkForAbandonment();
    	ok = nr.waitForCompletion(nsRequestTimeoutMillis);
    	if (!ok) {
    		throw new ConvergenceException("getAllNamespaces failed");
    	} else {
        	Log.warningAsyncf("Received all namespaces %s\n%s", targetRing.getRingIDAndVersionPair(), CollectionUtil.toString(nr.getNamespaces()));    	
    		return nr.getNamespaces();
    	}
    }    
    
	public void handleNamespaceResponse(MessageGroup message, MessageGroupConnection connection) {
        Set<Long>			namespaces;
        
        if (debug) {
            Log.warningAsync("Received namespace response from: ", connection.getRemoteSocketAddress());
        }
        namespaces = ImmutableSet.copyOf(ProtoNamespaceResponseMessageGroup.getNamespaces(message));
    	nr.peerComplete(connection.getRemoteIPAndPort(), namespaces);
	}    
    
    //////////////////////////////////////////////////////////////////
    
    private static final int	checksumTreeRequestTimeoutMinutes = 5;

    private void sendChecksumTreeRequest(ChecksumTreeRequest ctr, long ns, IPAndPort newOwner) throws ConvergenceException {
        MessageGroup    mg;
        UUIDBase		uuid;
        boolean	complete;
    	
        uuid = new UUIDBase(false);
        opUUIDs.add(uuid);
        mg = new ProtoChecksumTreeRequestMessageGroup(uuid, ns, ctr.getTargetCP(), ctr.getCurCP(),  
                                                      mgBase.getMyID(), ctr.getRegion(), ctr.getReplica(), true).toMessageGroup(); 
        if (verbose || debug) {
            System.out.printf("%x requestChecksumTree: %s\t%s\t%s\t%s\n", ns, ctr.getReplica(), ctr.getRegion(), ctr.getTargetCP(), ctr.getCurCP());
        }
        checkForAbandonment();
        mgBase.send(mg, newOwner);
        ctr.setSent();
        
        checkForAbandonment();        
        complete = opCompletionTracker.waitForCompletion(uuid, checksumTreeRequestTimeoutMinutes, TimeUnit.MINUTES);
    	opUUIDs.remove(uuid);
        if (!complete) {
        	throw new ConvergenceException("sendChecksumTreeRequest failed: "+ ctr.getReplica());
        }
    }
    
    private void syncReplica(long ns, RingRegion region, IPAndPort newOwner, IPAndPort oldOwner) throws ConvergenceException {
    	if (!newOwner.equals(oldOwner)) {
    		/*
	    	Log.warningAsyncf("syncReplica %x %s\t%s <- %s", ns, region, newOwner, oldOwner);
	    	sendChecksumTreeRequest(new ChecksumTreeRequest(targetCP, curCP, region, oldOwner), ns, newOwner);
	    	Log.warningAsyncf("Done syncReplica %x %s\t%s <- %s", ns, region, newOwner, oldOwner);
	    	*/
    		syncController.queueRequest(new ReplicaSyncRequest(ns, region, newOwner, oldOwner));
    	}
    }
       
    private void syncRegion(long ns, RingEntry targetEntry) throws ConvergenceException {
        List<RingEntry> sourceEntries;
        
    	Log.warningAsyncf("syncRegion %x %s", ns, targetEntry);
        sourceEntries = curMap.getEntries(targetEntry.getRegion());
        if (!sourceEntries.isEmpty()) {
            Log.warningAsyncf("%x target %s\towners %s\n", ns, targetEntry.getRegion(), CollectionUtil.toString(sourceEntries));
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
            	Log.warningAsyncf("Intersection %s %s", sourceEntry.getRegion(), targetEntry.getRegion());
            	iResult = RingRegion.intersect(sourceEntry.getRegion(), targetEntry.getRegion());
            	for (RingRegion commonSubRegion : iResult.getOverlapping()) {
            		for (IPAndPort newOwner : targetEntry.getOwnersIPList(OwnerQueryMode.Primary)) {
            			if (sourceOwners.contains(newOwner) && !syncUnchangedOwners) {
                    		Log.warningAsyncf("Skipping unchanged owner: %s", newOwner);
            			} else {
		            		for (IPAndPort sourceOwner : nonExcludedSourceOwners) {
                        		syncReplica(ns, commonSubRegion, newOwner.port(dhtConfig.getPort()), sourceOwner.port(dhtConfig.getPort()));
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
    	Log.warningAsyncf("Done syncRegion %x %s", ns, targetEntry);
    }    
    
    private void syncNamespace(long ns) throws ConvergenceException {
    	Set<RingEntry>	entries;
    	
    	Log.warningAsyncf("Synchronizing %x", ns);
    	entries = targetReplicaMap.getEntries();
    	for (RingEntry entry : entries) {
    		syncRegion(ns, entry);
    	}
    	Log.warningAsyncf("Done synchronizing %x", ns);
    }
    
    public void abandon() {
		abandoned = true;
		syncController.abandon();
		waitForCompletion();
    }
    
    private void checkForAbandonment() throws ConvergenceException {
    	if (abandoned) {
    		throw new ConvergenceException("Abandoned");
    	}
    }
    
    private void setComplete() {
    	completionLock.lock();
    	try {
    		complete = true;
    		completionCV.signalAll();
    	} finally {
    		completionLock.unlock();
    	}
    }
    
    private void waitForCompletion() {
    	completionLock.lock();
    	try {
    		while (!complete) {
    			try {
					completionCV.await();
				} catch (InterruptedException e) {
				}
    		}
    	} finally {
    		completionLock.unlock();
    	}
    }
    
    public void syncAll(Set<Long> namespaces) throws ConvergenceException {
    	Log.warningAsync("Synchronizing namespaces");
    	for (long ns : namespaces) {
    		syncNamespace(ns);
    	}
    	Log.warningAsync("Done synchronizing namespaces");
    	
    	syncController.freeze();
    	
    	Log.warningAsync(" *** Sending requests");
    	syncController.waitForCompletion(1, TimeUnit.DAYS); // FUTURE - improve this from a failsafe to a real limit
    	Log.warningAsync(" *** Requests complete");
    }
    
    public void converge() throws ConvergenceException {
    	try {
	    	Set<Long>	namespaces;
	    	
	    	Log.warningAsync("Starting convergence ", targetRing.getRingIDAndVersionPair());
	    	setNodesState(RingState.INITIAL);
	    	namespaces = getAllNamespaces();
	    	// need to send all namespaces out to the nodes
	    	setNodesState(RingState.READY_FOR_CONVERGENCE_1);
	    	// sleep here to allow in flight operations to complete on the old replica sets
	    	setNodesState(RingState.READY_FOR_CONVERGENCE_2);
	    	
	    	syncAll(namespaces);
	    	
	    	setNodesState(RingState.LOCAL_CONVERGENCE_COMPLETE_1);
	    	setNodesState(RingState.ALL_CONVERGENCE_COMPLETE_1);
	    	setNodesState(RingState.ALL_CONVERGENCE_COMPLETE_2);
	    	setNodesState(RingState.CLOSED);
	    	Log.warningAsync("Convergence complete", targetRing.getRingIDAndVersionPair());
    	} catch (ConvergenceException ce) {
    		boolean	abandonSetAll;
    		
    		Log.logErrorWarning(ce, "Convergence failed"+ targetRing.getRingIDAndVersionPair());
    		try {
    			setNodesState(RingState.ABANDONED);
    			abandonSetAll = true;
    		} catch (ConvergenceException ce2) {
    			abandonSetAll = false;
    		}
    		if (abandonSetAll) {
    			Log.warningAsync("setNodesState(RingState.ABANDONED) succeeded. Previous failure was transient."); 
    		}
    		throw ce;
    	} finally {
    		setComplete();
    	}
    }
    
	///////////////////////////////////////////////////
	
	private void ensureUUIDMatches(UUIDBase uuid) {
		if (!uuid.equals(this.uuid)) {
			throw new RuntimeException("Unexpected UUID: "+ uuid +" != "+ this.uuid);
		}
	}

	@Override
	public void stop(UUIDBase uuid) {
		ensureUUIDMatches(uuid);
	}

	@Override
	public void waitForCompletion(UUIDBase uuid) {
		ensureUUIDMatches(uuid);
	}

	@Override
	public RequestStatus getStatus(UUIDBase uuid) {
		ensureUUIDMatches(uuid);
		if (ringState != null) {
			switch (ringState) {
			case READY_FOR_CONVERGENCE_2:
				return new SimpleRequestStatus(ringState.isFinal(), ringState.toString() +":"+ syncController.getStatus().toString());
			default:
				return new SimpleRequestStatus(ringState.isFinal(), ringState.toString());
			}
		} else {
			return new SimpleRequestStatus(false, "<init>");
		}
	}
	
	//////////////////////////////////////////////////////////////////
	
	public static void main(String[] args) {
		try {
	        CmdLineParser       parser;
	        CentralConvergenceControllerOptions    options;
	        CentralConvergenceController   ccc;
	        SKGridConfiguration gc;
	        
	        options = new CentralConvergenceControllerOptions();
	        parser = new CmdLineParser(options);
	        try {
	            parser.parseArgument(args);
	            gc = SKGridConfiguration.parseFile(options.gridConfig);
	        } catch (CmdLineException cle) {
	            System.err.println(cle.getMessage());
	            parser.printUsage(System.err);
	            System.exit(-1);
	        }
		} catch (Exception e) {
			System.exit(-1);
		}
	}
}

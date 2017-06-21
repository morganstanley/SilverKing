package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.common.CorruptValueException;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.DHTNode;
import com.ms.silverking.cloud.dht.daemon.RingMapState;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.dht.daemon.storage.KeyedOpResultListener;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceConvergenceGroup;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoRetrievalMessageGroup;
import com.ms.silverking.cloud.meta.ExclusionSet;
import com.ms.silverking.cloud.ring.IntersectionResult;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.toporing.RingEntry;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.Connection;
import com.ms.silverking.net.async.MultipleConnectionQueueLengthListener;
import com.ms.silverking.numeric.LongInterval;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.time.AbsMillisTimeSource;

/**
 * ConvergenceController controls replica consistency for both primary replication and secondary replication. 
 * This includes consistency for topology changes.
 */
public class ConvergenceController2 implements KeyedOpResultListener, Comparable<ConvergenceController2> {
    private final long                  ns;
    private final NamespaceStore        nsStore;
    private final MessageGroupBase      mgBase;
    //private final NodeRingMaster        ringMaster;
    private final ChecksumTreeServer    checksumTreeServer;
    private OwnerQueryMode  ownerQueryMode;
    private UUIDBase    myUUID;
    private final Map<UUIDBase,ChecksumTreeRequest>   outstandingChecksumTreeRequests;
    private final Map<UUIDBase,SyncRetrievalRequest>   outstandingSyncRetrievalRequests;
    private final ConcurrentLinkedQueue<ChecksumTreeRequest>	queuedChecksumTreeRequests;
    private final NamespaceConvergenceGroup ncg;
    private final ConvergencePoint targetCP;
    private final ConvergencePoint curCP;
    private final RingMapState targetMapState;
    private final RingMapState curMapState;
    private final ExclusionSet exclusionSet;
    
    private final Lock				chainNextLock;
    private ConvergenceController2	chainNext;
    private boolean					chainNextSignaled;
        
    ////////
    
    private static final byte[] emptyUserData = new byte[0];
    
    private static final long   checksumVCMin = Long.MIN_VALUE + 1;
    
    private static final boolean    debug = false;
    private static final boolean    verbose = true;
    
    private static final int    convergenceRelativeDeadlineMillis = 10 * 60 * 1000;
    private static final int    retrievalBatchSize = 256;
    
    protected static AbsMillisTimeSource  absMillisTimeSource;
    
    public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
        absMillisTimeSource = _absMillisTimeSource;
    }
    
    private static VersionConstraint checksumVersionConstraint(long max) {
        return new VersionConstraint(checksumVCMin, Long.MAX_VALUE, VersionConstraint.Mode.GREATEST);
        //return new VersionConstraint(checksumVCMin, max, VersionConstraint.Mode.NEWEST);
        //return VersionConstraint.newest;
    }
    
    public static boolean isChecksumVersionConstraint(VersionConstraint vc) {
        return vc.getMin() == checksumVCMin;
    }

    public ConvergenceController2(NamespaceStore nsStore, MessageGroupBase mgBase, 
                                  ChecksumTreeServer checksumTreeServer, NamespaceConvergenceGroup ncg,
                                  OwnerQueryMode ownerQueryMode, 
                                  ConvergencePoint targetCP, ConvergencePoint curCP,
                                  RingMapState targetMapState, RingMapState curMapState,
                                  ExclusionSet exclusionSet) {
        myUUID = new UUIDBase(false);
        this.ns = nsStore.getNamespace();
        this.nsStore = nsStore;
        this.mgBase = mgBase;
        this.checksumTreeServer = checksumTreeServer;
        this.ncg = ncg;
        this.ownerQueryMode = ownerQueryMode;
        this.targetCP = targetCP;
        this.curCP = curCP;
        Preconditions.checkNotNull(targetMapState);
        Preconditions.checkNotNull(curMapState);
        this.targetMapState = targetMapState;
        this.curMapState = curMapState;
        this.exclusionSet = exclusionSet;
        chainNextLock = new ReentrantLock();
        ncg.addConvergenceController(this);
        //ringMaster = nsStore.getRingMaster();
        outstandingChecksumTreeRequests = new ConcurrentHashMap<>();
        outstandingSyncRetrievalRequests = new ConcurrentHashMap<>();
        queuedChecksumTreeRequests = new ConcurrentLinkedQueue<>();
        Log.warningf("Created convergence controller %x %s", ns, myUUID);
        activeConvergenceControllers.add(this);
    }
    
    public UUIDBase getMyUUID() {
        return myUUID;
    }
    
    public OwnerQueryMode getOwnerQueryMode() {
        return ownerQueryMode;
    }
    
    @Override
    public boolean equals(Object o) {
        ConvergenceController2  oConvergenceController;
        
        oConvergenceController = (ConvergenceController2)o;
        return myUUID.equals(oConvergenceController.myUUID);
    }
    
    @Override
    public int hashCode() {
        return myUUID.hashCode();
    }
    

    @Override
    public int compareTo(ConvergenceController2 o) {
        return myUUID.compareTo(o.myUUID);
    }
    
    private static final int    mapDisplayThreshold = 15;
    
    private void displayOutstandingRequests(Map<UUIDBase,? extends Object> map, String name) {
        if (map.size() < mapDisplayThreshold) {
            System.out.printf("%s:\t%d\n", name, map.size());
            for (Map.Entry<UUIDBase,? extends Object> entry : map.entrySet()) {
                System.out.printf("%s\t%s\n", entry.getKey(), entry.getValue());
            }
        }
    }
    
    private void displayState() {
        Log.warning(super.toString() +"\t"+ outstandingChecksumTreeRequests.size() 
                    +"\t"+ outstandingSyncRetrievalRequests.size());
        displayOutstandingRequests(outstandingChecksumTreeRequests, "outstandingChecksumTreeRequests");
        displayOutstandingRequests(outstandingSyncRetrievalRequests, "outstandingSyncRetrievalRequests");
        Log.warningf("queuedChecksumTreeRequests:\t%d\n", queuedChecksumTreeRequests.size());
        Log.warning(String.format("convergenceControllers %d\tactiveConvergenceControllers %d\n", 
                convergenceControllers.size(), activeConvergenceControllers.size()));
    }
    
    //////////////////////////////////////////////////////////////////////
    // Generate requests for convergence
    
    public void setChainNext(ConvergenceController2 chainNext) {
    	chainNextLock.lock();
    	try {
	    	assert this.chainNext == null;
	    	this.chainNext = chainNext;
			Log.warning(String.format("%s setting chainNext %s", myUUID, chainNext.myUUID));
    	} finally {
    		chainNextLock.unlock();
    	}
    }

    private void signalChainNext() {
		chainNextLock.lock();
		try {
			if (chainNext != null) {
				if (!chainNextSignaled) {
					Log.warning(String.format("%s signaling chainNext %s", myUUID, chainNext.myUUID));
					chainNextSignaled = true;
					chainNext.startConvergence();
				} else {
					Log.warning(String.format("%s chainNext already signaled", myUUID));
				}
			} else {
				Log.warning(String.format("%s chainNext is null", myUUID));
			}
		} finally {
			chainNextLock.unlock();
		}
    }
    
    public void startConvergence() {
    	convergencePauseMutex.lock();
    	try {
    		if (convergencePaused) {
    			try {
    				Log.warning("Storing paused signalled convergence controller ", myUUID);
					pausedSignalledControllers.put(this);
				} catch (InterruptedException e) {
				};
    			return;
    		}
    	} finally {
        	convergencePauseMutex.unlock();
    	}
		Log.warningf("startConvergence %x %s", ns, myUUID);
        this.ownerQueryMode = ownerQueryMode;
        switch (ownerQueryMode) {
        case Primary: 
            startPrimaryConvergence(); 
            break;
        case Secondary: 
            startSecondaryConvergence(); 
            break;
        default: throw new RuntimeException("panic");
        }
        displayState();
        checkForCompletion();
    }
    
    public List<RingEntry> getEntries(RingRegion region) {
        return curMapState.getResolvedReplicaMap().getEntries(region);
    }
    
    public List<RingEntry> getCurrentMapReplicaEntries(IPAndPort replica, OwnerQueryMode oqm) {
        return curMapState.getResolvedReplicaMap().getReplicaEntries(replica, oqm);
    }
    
    public List<RingEntry> getTargetMapReplicaEntries(IPAndPort replica, OwnerQueryMode oqm) {
        return targetMapState.getResolvedReplicaMap().getReplicaEntries(replica, oqm);
    }
    
    private void startPrimaryConvergence() {
        List<RingEntry> myPrimaryEntries;
        
        myPrimaryEntries = getTargetMapReplicaEntries(mgBase._getIPAndPort(), OwnerQueryMode.Primary);
        if (myPrimaryEntries != null) {
            for (RingEntry entry : myPrimaryEntries) {
                requestRemoteChecksumTreesForPrimary(entry);
            }
        }
    }
    
    /*
     * Currently this code only requests checksums for regions where
     * the local node is a secondary replica.
     */
    private void startSecondaryConvergence() {
        List<RingEntry> mySecondaryEntries;
        
        mySecondaryEntries = getCurrentMapReplicaEntries(mgBase._getIPAndPort(), OwnerQueryMode.Secondary);
        if (mySecondaryEntries != null) {
            for (RingEntry entry : mySecondaryEntries) {
                requestRemoteChecksumTreeForSecondary(entry);
            }
        }
    }
    
    private IPAndPort getFirstNonLocalPrimary(RingEntry entry) {
        for (Node node : entry.getPrimaryOwnersList()) {
            IPAndPort   primary;
            
            primary = new IPAndPort(node.getIDString(), DHTNode.getServerPort());
            if (!primary.equals(mgBase._getIPAndPort())) {
                return primary;
            }
        }
        return null;
    }
    
    private Set<IPAndPort> getAllNonLocalPrimary(RingEntry entry) {
        ImmutableSet.Builder<IPAndPort> pSetBuilder;
        
        pSetBuilder = ImmutableSet.builder();
        for (Node node : entry.getPrimaryOwnersList()) {
            IPAndPort   primary;
            
            primary = new IPAndPort(node.getIDString(), DHTNode.getServerPort());
            if (!primary.equals(mgBase._getIPAndPort())) {
                pSetBuilder.add(primary);
            }
        }
        return pSetBuilder.build();
    }
    
    /*
    private void requestRemoteChecksumTreesForPrimary(ConvergencePoint targetCP, ConvergencePoint curCP, RingEntry targetEntry) {
        Set<IPAndPort>  sourceOwners;
        
        sourceOwners = ringMaster.getReplicas(targetEntry.getRegion(), OwnerQueryMode.Primary);
        //sourceOwners = ImmutableUtil.remove(sourceMap.getOwners(targetEntry.getRegion(), OwnerQueryMode.Primary), 
        //                                    mgBase._getIPAndPort());
        if (!sourceOwners.isEmpty()) {
            if (true) {
                System.out.printf("target %s\towners %s\n", targetEntry.getRegion(), CollectionUtil.toString(sourceOwners));
            }
            requestRemoteChecksumTree(targetCP, curCP, targetEntry, sourceOwners);
        } else {
            Log.warning("Primary convergence. No previous non-local owners for entry: ", targetEntry);
        }
    }
    */
    
    private void requestRemoteChecksumTreesForPrimary(RingEntry targetEntry) {
        List<RingEntry> sourceEntries;
        
        sourceEntries = getEntries(targetEntry.getRegion());
        if (!sourceEntries.isEmpty()) {
            Log.warningf("%x target %s\towners %s\n", ns, targetEntry.getRegion(), CollectionUtil.toString(sourceEntries));
            for (RingEntry sourceEntry : sourceEntries) {
                List<IPAndPort> nonLocalOwners;
                List<IPAndPort> _nonLocalOwners;
                //ExclusionSet    curExclusionSet;
                
                nonLocalOwners = new ArrayList<>(sourceEntry.getOwnersIPList(OwnerQueryMode.Primary));
                nonLocalOwners.remove(mgBase._getIPAndPort());
                
                //curExclusionSet = ringMaster.getCurrentExclusionSet();
                //Log.warning("Filtering exclusion set: ", curExclusionSet);
                //nonLocalOwners = curExclusionSet.filterByIP(nonLocalOwners);
                Log.info("Filtering exclusion set: ", exclusionSet);
                _nonLocalOwners = exclusionSet.filterByIP(nonLocalOwners);
                if (_nonLocalOwners.size() != nonLocalOwners.size()) {
                    Log.warning("Raw nonLocalOwners:      ", nonLocalOwners);
                    Log.warning("Filtered nonLocalOwners: ", _nonLocalOwners);
                }
                
                if (_nonLocalOwners.size() == 0) {
                	Log.warningf("%x All nonLocalOwners excluded. Ignoring exclusions for this entry.", ns);
                } else {
                	nonLocalOwners = _nonLocalOwners;
                }
                
            	IntersectionResult	iResult;
            	
            	// We don't want to request the entire source region.
            	// We're only interested in the portion(s) of the source region that cover(s) the target region.                	
            	iResult = RingRegion.intersect(sourceEntry.getRegion(), targetEntry.getRegion());
            	for (RingRegion commonSubRegion : iResult.getOverlapping()) {
            		requestRemoteChecksumTree(targetCP, curCP, commonSubRegion, nonLocalOwners);
            	}
            }
        } else {
        	// FIXME - this should actually never occur as the getEntries() call above
        	// has no notion of local/non-local. It just returns the owners, and there 
        	// should always be owners.
            Log.warningf("Primary convergence %x. No previous non-local owners for entry: ", ns, targetEntry);
        }
    }
    
    
    private void requestRemoteChecksumTreeForSecondary(RingEntry entry) {
        IPAndPort       primary;
        
        primary = getFirstNonLocalPrimary(entry);
        if (primary != null) {
            requestRemoteChecksumTree(targetCP, targetCP, entry.getRegion(), ImmutableSet.of(primary));
        } else {
            Log.warning("Secondary convergence. No primary replicas for entry: ", entry);
        }
    }
    
    static final long	checksumTreeRequestTimeout = 1 * 60 * 1000;
    
    private void requestRemoteChecksumTree(ConvergencePoint targetCP, ConvergencePoint curCP, RingRegion region, 
                                           Collection<IPAndPort> replicas) {
        for (IPAndPort replica : replicas) {
            /*
             * Generate a request per replica for primary.
             * For secondary, do something like the current implementation, but pick a random primary replica
             * unless the replicas are already sorted by distance...they probably should be already
             * 
             * In either case, we track completion by uuid.
             * 
             * Need to measure the scaling of the current implementation since we require the entire tree to
             * fit into a message. Better would be to request sub regions etc. We can wait until this
             * implementation is working for primaries and secondaries before implementing that improvement.
             */
            
            if (!replica.equals(mgBase._getIPAndPort())) {
            	queuedChecksumTreeRequests.add(new ChecksumTreeRequest(targetCP, curCP, region, replica));
            	//sendChecksumTreeRequest(targetCP, curCP, region, replica);
            }
        }
        checkQueuedChecksumTreeRequests();
    }
    
    private void checkQueuedChecksumTreeRequests() {
        if (outstandingChecksumTreeRequests.isEmpty() && outstandingSyncRetrievalRequests.isEmpty()) {
        	ChecksumTreeRequest	ctr;
        	
        	ctr = queuedChecksumTreeRequests.poll();
        	if (ctr != null) {
        		Log.warningf("%x checkQueuedChecksumTreeRequests found ChecksumTreeRequest", ns);
        		sendChecksumTreeRequest(ctr);
        	} else {
        		Log.warningf("%x checkQueuedChecksumTreeRequests - no requests queued", ns);
        	}
        } else {
    		Log.warningf("%x checkQueuedChecksumTreeRequests waiting for ongoing requests", ns);
        }
    }
    
    private void sendChecksumTreeRequest(ChecksumTreeRequest ctr) {
        MessageGroup    mg;
        UUIDBase        uuid;
    	
        uuid = UUIDBase.random();
        outstandingChecksumTreeRequests.put(uuid, ctr); 
        convergenceControllers.put(uuid, this);
        mg = new ProtoChecksumTreeRequestMessageGroup(uuid, ns, ctr.getTargetCP(), ctr.getCurCP(),  
                                                      mgBase.getMyID(), ctr.getRegion(), false).toMessageGroup(); 
        if (verbose || debug) {
        	Log.warningAsyncf("%x requestChecksumTree: %s\t%s\t%s\t%s", ns, ctr.getReplica(), ctr.getRegion(), ctr.getTargetCP(), ctr.getCurCP());
        }
        mgBase.send(mg, ctr.getReplica());
        ctr.setSent();
    }
    
    //////////////////////////////////////////////////////////////////////
    // Handle incoming convergence trees
    
    class SyncRetrievalRequest {
    	final UUIDBase		uuid;
    	final Set<DHTKey>	outstandingKeys;
    	final long	dataVersion;
    	final MessageGroupConnection connection;
    	long sendTime;
    	
    	SyncRetrievalRequest(UUIDBase uuid, Set<DHTKey> outstandingKeys, long dataVersion, MessageGroupConnection connection) {
    		this.uuid = uuid;
    		this.outstandingKeys = outstandingKeys;
    		this.dataVersion = dataVersion;
    		this.connection = connection;
    	}
    	
    	void setSent() {
    		sendTime = absMillisTimeSource.absTimeMillis();
    	}
    	
    	boolean hasTimedOut() {
    		return absMillisTimeSource.absTimeMillis() > sendTime + checksumTreeRequestTimeout;
    	}
    	
    	@Override
    	public String toString() {
    		return connection.getRemoteIPAndPort() +" "+ outstandingKeys.size();
    	}
    }
    
    //private final ConcurrentMap<ConvergencePoint,Queue<DHTKey>> keysToFetchMap = new ConcurrentHashMap<>();
    //private final Queue<DHTKey>   keysToFetchQueue = new ConcurrentLinkedQueue<>();
    private final Queue<OutgoingMessage> outgoingMessages = new ConcurrentLinkedQueue<>();
    private final AtomicLong lastMGQueueReset = new AtomicLong();
    private final AtomicInteger outstandingMessages = new AtomicInteger();
    private static final int    maxOutstandingMessages = 256;
    private static final int    outstandingMessagesResetInterval = 60 * 1000;
    
    // FUTURE - ENSURE THAT NO CONVERGENCE CODE USES THE RECEIVE THREADS TO DO WORK
    // USE A WORKER
    
    private void incomingChecksumTree(UUIDBase incomingUUID, ConvergencePoint cp,
                                      ChecksumNode remoteTree, MessageGroupConnection connection) {
        ChecksumNode    localTree;
        MatchResult     matchResult;
        Set<DHTKey>     keysToFetch;
        List<DHTKey>    keysToFetchList;

        outstandingChecksumTreeRequests.remove(incomingUUID);
        checkQueuedChecksumTreeRequests();
        if (remoteTree == null) {
        	checkForCompletion();
            return;
        }
        localTree = checksumTreeServer.getRegionChecksumTree_Local(cp, 
                                        remoteTree.getRegion(), new LongInterval(Long.MIN_VALUE, cp.getDataVersion()));
        if (verbose) {
        	Log.warningAsyncf("incomingChecksumTree %s", incomingUUID);
        }
        if (debug) {
        	Log.warningAsyncf("incomingChecksumTree %x\t%s\t%s\t%s", ns, cp, remoteTree.getRegion(), incomingUUID);
        	Log.warningAsyncf(remoteTree +"\n\nlocalTree\n"+ localTree);
        }
        if (localTree == null) {
        	checkForCompletion();
            return;
        }
        try {
        matchResult = TreeMatcher.match(localTree, remoteTree);
        } catch (RuntimeException re) {
            System.err.println(localTree);
            System.err.println();
            System.err.println(connection);
            System.err.println(remoteTree);
            throw re;
        }
        if (debug) {
        	Log.warningAsyncf("%s", matchResult);
        }
        keysToFetch = new HashSet<>();
        for (KeyAndVersionChecksum kvc : matchResult.getDestNotInSource()) {
            if (debug) {
                System.out.printf("Adding destNotInSource %s\n", kvc.getKey());
            }
            keysToFetch.add(kvc.getKey());
        }
        for (KeyAndVersionChecksum kvc : matchResult.getChecksumMismatch()) {
            if (debug) {
                System.out.printf("Adding checksumMismatch %s\n", kvc.getKey());
            }
            keysToFetch.add(kvc.getKey());
        }
        /*
        Queue<DHTKey>   keysToFetchQueue;
        
        keysToFetchQueue = keysToFetchMap.get(cp);
        if (keysToFetchQueue == null) {
            Queue<DHTKey>   prev;
            
            keysToFetchQueue = new ConcurrentLinkedQueue<>();
            prev = keysToFetchMap.putIfAbsent(cp, keysToFetchQueue);
            if (prev != null) {
                keysToFetchQueue = prev;
            }
        }
        keysToFetchQueue.addAll(keysToFetch);
        */
        keysToFetchList = new LinkedList<>(keysToFetch);
        while (keysToFetchList.size() > 0) {
            Set<DHTKey> batchKeys;
            int         batchSize;
            RetrievalOptions    retrievalOptions;
            UUIDBase	uuid;
            //MessageGroup        mg;
            SyncRetrievalRequest	srr;
            
            //batchKeys = new HashSet<>(retrievalBatchSize);
            batchKeys = new ConcurrentSkipListSet<DHTKey>();
            batchSize = 0;
            while (keysToFetchList.size() > 0 && batchSize < retrievalBatchSize) {
                batchKeys.add(keysToFetchList.remove(0));
                ++batchSize;
            }
            
            // FUTURE - could consider sending SourceNotInDest
            retrievalOptions = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE_AND_META_DATA, WaitMode.GET,
                                                    checksumVersionConstraint(cp.getDataVersion()));
                    //new VersionConstraint(Long.MIN_VALUE + 1, version, Mode.NEWEST)); 
            uuid = UUIDBase.random();
            srr = new SyncRetrievalRequest(uuid, batchKeys, cp.getDataVersion(), connection);
            outstandingSyncRetrievalRequests.put(uuid, srr); 
            convergenceControllers.put(uuid, this);
            sendSyncRetrievalRequest(srr);
            //mg = new ProtoRetrievalMessageGroup(uuid, ns, new InternalRetrievalOptions(retrievalOptions),
            //        mgBase.getMyID(), batchKeys, convergenceRelativeDeadlineMillis).toMessageGroup();
            //outgoingMessages.add(new OutgoingMessage(mg, new IPAndPort(connection.getRemoteSocketAddress())));
            /*
            try {
                connection.sendAsynchronous(mg, mg.getDeadlineAbsMillis(absMillisTimeSource));
            } catch (IOException ioe) {
                Log.logErrorWarning(ioe);
            }
            */
            //if (keysToFetchList.size() > 0) {
            //    ThreadUtil.sleep(5);
            //}
        }
        checkMGQueue();
        if (debug) {
        	Log.warningAsyncf("no more keysToFetch");
        }
        checkForCompletion();
    }
    
    private void sendSyncRetrievalRequest(SyncRetrievalRequest srr) {
        MessageGroup		mg;
        RetrievalOptions    retrievalOptions;
        
        retrievalOptions = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE_AND_META_DATA, WaitMode.GET,
                checksumVersionConstraint(srr.dataVersion));
        mg = new ProtoRetrievalMessageGroup(srr.uuid, ns, new InternalRetrievalOptions(retrievalOptions),
                mgBase.getMyID(), srr.outstandingKeys, convergenceRelativeDeadlineMillis).toMessageGroup();
        outgoingMessages.add(new OutgoingMessage(mg, new IPAndPort(srr.connection.getRemoteSocketAddress())));
    }
    
    private void checkMGQueue() {
        OutgoingMessage m;
        long            curTime;
        
        // at least send 1, possibly more if permitted
        do {
            m = outgoingMessages.poll();
            if (m != null) {
                mgBase.send(m.mg, m.dest);
                outstandingMessages.incrementAndGet();
            }
        } while (m != null && outstandingMessages.get() < maxOutstandingMessages);
        curTime = SystemTimeUtil.systemTimeSource.absTimeMillis();
        if (curTime - lastMGQueueReset.get() > outstandingMessagesResetInterval) {
            outstandingMessages.set(0);
            lastMGQueueReset.set(curTime);
        }
    }
    
    class OutgoingMessage {
        final MessageGroup  mg;
        final IPAndPort     dest;
        
        OutgoingMessage(MessageGroup mg, IPAndPort dest) {
            this.mg = mg;
            this.dest = dest;
        }
    }

    /*
    private void fetchKeys() {
        Set<DHTKey> batchKeys;
        DHTKey      key;
        
        batchKeys = new HashSet<>(retrievalBatchSize);
        do {
            key = keysToFetchQueue.poll();
            if (key != null) {
                batchKeys.add(key);
            }
        } while (key != null && batchKeys.size() < retrievalBatchSize);
        
        if (batchKeys.size() > 0) {
            RetrievalOptions    retrievalOptions;
            MessageGroup        mg;
            UUIDBase            uuid;
            
            // FUTURE - could consider sending SourceNotInDest
            retrievalOptions = new RetrievalOptions(RetrievalType.VALUE_AND_META_DATA, WaitMode.GET,
                                                    checksumVersionConstraint(cp.getDataVersion()));
                    //new VersionConstraint(Long.MIN_VALUE + 1, version, Mode.NEWEST)); 
            uuid = new UUIDBase(true);
            outstandingRequests.add(uuid); 
            convergenceControllers.put(uuid, this);
            mg = new ProtoRetrievalMessageGroup(uuid, ns, new InternalRetrievalOptions(retrievalOptions),
                    mgBase.getIPAndPort(), batchKeys, convergenceRelativeDeadlineMillis).toMessageGroup();
            try {
                mgBase.send(mg, dest);
                connection.sendAsynchronous(mg, mg.getDeadlineAbsMillis(absMillisTimeSource));
            } catch (IOException ioe) {
                Log.logErrorWarning(ioe);
            }
        } else {
            if (debug) {
                System.out.println("no more keysToFetch");
            }
            checkForCompletion();
        }
    }
    */
    
    //////////////////////////////////////////////////////////////////////
    // completion check
    
    private void checkForCompletion() {
        if (outstandingChecksumTreeRequests.isEmpty() && outstandingSyncRetrievalRequests.isEmpty() && queuedChecksumTreeRequests.isEmpty()) {
            boolean removed;
            
            removed = activeConvergenceControllers.remove(this);
            if (removed) {
            	Log.warningAsyncf(String.format("ConvergenceController is complete: %s\t%x\n", ownerQueryMode, ns));
                ncg.setComplete(this);
                activeConvergenceControllers.remove(this);
                convergenceControllers.remove(myUUID);
            }
            signalChainNext();
        }
        displayState();
    }
    
    //////////////////////////////////////////////////////////////////////
    // Handle incoming convergence data
    
    public void incomingSyncRetrievalResponse(MessageGroup message) {
        List<StorageValueAndParameters> svpList;
        SyncRetrievalRequest	srr;
        
        srr = outstandingSyncRetrievalRequests.get(message.getUUID());
        
        outstandingMessages.decrementAndGet();
        if (outstandingMessages.get() < 0) {
            outstandingMessages.set(0);
        }
        
        if (debug) {
        	Log.warningAsyncf("incomingSyncRetrievalResponse");
        }
        svpList = new ArrayList<>();
        for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator()) {
            StorageValueAndParameters   svp;
            
            if (srr != null) {
            	srr.outstandingKeys.remove(entry);
            }
            svp = StorageValueAndParameters.createSVP(entry);
            if (svp != null) {
                svpList.add(svp);
            }
        }
        if (!svpList.isEmpty()) {
        	// FUTURE - support migration of user data
            nsStore.put(svpList, emptyUserData, this);
        }
        if (srr != null && srr.outstandingKeys.isEmpty()) {
            outstandingSyncRetrievalRequests.remove(message.getUUID());
        }
        checkQueuedChecksumTreeRequests();
        checkMGQueue();
        checkForCompletion();
    }
    
    // Currently unused
    private void incomingSyncRetrievalResponse(MessageGroupRetrievalResponseEntry entry) {
        try {
            RawRetrievalResult  rawRetrievalResult;
            
            if (entry.getValue() == null) {
                Log.warning("Couldn't find value for: ", entry);
                return;
            } else {
                if (debug) {
                    System.out.printf("Found %s\n", entry);
                }
            }
            rawRetrievalResult = new RawRetrievalResult(RetrievalType.VALUE_AND_META_DATA);
            rawRetrievalResult.setStoredValue(entry.getValue(), true, false, null);
            StorageValueAndParameters   valueAndParameters;
            
            ByteBuffer  rawValueBuffer;
            ByteBuffer  valueBuffer;
            
            //valueBuffer = (ByteBuffer)entry.getValue().duplicate().limit(rawRetrievalResult.getStoredLength());
            rawValueBuffer = entry.getValue();
            if (debug && true) {
                System.out.printf("key %s buf %s storedLength %d uncompressedLength %d compressedLength %d\n", 
                    entry, rawValueBuffer, rawRetrievalResult.getStoredLength(),  
                    rawRetrievalResult.getUncompressedLength(), MetaDataUtil.getCompressedLength(rawValueBuffer, 0));
                System.out.printf("rawValueBuffer %s\n", StringUtil.byteBufferToHexString(rawValueBuffer));
            }
            
            valueBuffer = (ByteBuffer)rawValueBuffer.duplicate().position(
                    rawValueBuffer.position() + MetaDataUtil.getDataOffset(rawValueBuffer, 0));
            
            // FUTURE - consider making the nsstore allow a put that just accepts the buffer as is
            // to improve performance
            
            valueAndParameters = new StorageValueAndParameters(entry, valueBuffer, 
                    rawRetrievalResult.getVersion(), 
                    rawRetrievalResult.getUncompressedLength(), 
                    MetaDataUtil.getCompressedLength(rawValueBuffer, 0), 
                    rawRetrievalResult.getCCSS(), rawRetrievalResult.getChecksum(), 
                    rawRetrievalResult.getCreator().getBytes(), rawRetrievalResult.getCreationTimeRaw());
            nsStore.put(ImmutableList.of(valueAndParameters), emptyUserData, this);
            
            // FUTURE - preserve user data
        } catch (CorruptValueException cve) {
            Log.logErrorWarning(cve);
            // FUTURE - let this replica know that it's bad
        }
    }    

    //KeyedOpResultListener implementation; only used for ignoring results of puts for now 
    public void sendResult(DHTKey key, OpResult result) {
    }
    
	private void cancelConvergence() {
		Log.warning("cancelConvergence() ", targetCP);
	    outstandingChecksumTreeRequests.clear();
	    outstandingSyncRetrievalRequests.clear();
	}
    
    ///////////////////////////////////////////////////////////////////////////
    
    private static final Set<ConvergenceController2>   activeConvergenceControllers;    
    private static final ConcurrentMap<UUIDBase, ConvergenceController2>    convergenceControllers;
    
    static {
        //convergenceControllers = new ConcurrentHashMap<UUIDBase, ConvergenceController2>();
        convergenceControllers = new MapMaker().weakValues().makeMap();
        activeConvergenceControllers = new ConcurrentSkipListSet<>();
    }
    
    public static void _incomingChecksumTree(UUIDBase uuid, ChecksumNode remoteTree, 
                                             ConvergencePoint cp, 
                                             MessageGroupConnection connection) {
    	convergencePauseMutex.lock();
    	try {
    		if (convergencePaused) {
    			try {
    				Log.warning("Storing paused checksum tree ", uuid);
					pausedChecksumTrees.put(new PausedChecksumTree(uuid, remoteTree, cp, connection));
				} catch (InterruptedException e) {
				};
    			return;
    		}
    	} finally {
        	convergencePauseMutex.unlock();
    	}
    	
        ConvergenceController2   convergenceController;
        
        convergenceController = convergenceControllers.get(uuid);
        if (convergenceController != null) {
            convergenceController.incomingChecksumTree(uuid, cp, remoteTree, connection);
        } else {
            Log.warning("No convergenceController for uuid: ", uuid);
        }
    }
    
    public static boolean _incomingSyncRetrievalResponse(MessageGroup message) {
        ConvergenceController2   convergenceController;
        
        convergenceController = convergenceControllers.get(message.getUUID());
        if (convergenceController != null) {
            convergenceController.incomingSyncRetrievalResponse(message);
            return true;
        } else {
            Log.info("No convergenceController for uuid: ", message.getUUID());
            return false;
        }
    }
    
    public static void cancelAllOngoingConvergence() {
		Log.warning("cancelAllOngoingConvergence()");
    	for (ConvergenceController2 cc2 : activeConvergenceControllers) {
    		cc2.cancelConvergence();
    	}
    	activeConvergenceControllers.clear();
    	convergenceControllers.clear();
    	pausedChecksumTrees.clear();
    	pausedSignalledControllers.clear();
    }
    
    ///////////////////////////////////////////////////////////////////////////
    
    private static boolean	convergencePaused;
    private static Lock		convergencePauseMutex = new ReentrantLock();
    private static Condition	convergencePauseCV = convergencePauseMutex.newCondition();
    
    private static BlockingQueue<PausedChecksumTree>		pausedChecksumTrees = new LinkedBlockingQueue<>();
    private static BlockingQueue<ConvergenceController2>	pausedSignalledControllers = new LinkedBlockingQueue<>();
    
    //private static final int	queuePauseThreshold = 100;
    //private static final int	queueUnpauseThreshold = 50;
    private static final int	queuePauseThreshold = 50;
    private static final int	queueUnpauseThreshold = 25;
    
    static class PausedChecksumTree {
    	final UUIDBase 					uuid;
    	final ChecksumNode				remoteTree; 
        final ConvergencePoint			cp; 
        final MessageGroupConnection	connection;
        
        PausedChecksumTree(UUIDBase uuid, ChecksumNode remoteTree, 
                ConvergencePoint cp, 
                MessageGroupConnection connection) {
        	this.uuid = uuid;
        	this.remoteTree = remoteTree; 
        	this.cp = cp;
        	this.connection = connection;
        }
    }
    
    public static void pauseConvergence() {
    	convergencePauseMutex.lock();
    	try {
    		if (!convergencePaused) {
				Log.warning("Pausing convergence");
    			convergencePaused = true;
    			convergencePauseCV.signalAll();
    		}
    	} finally {
        	convergencePauseMutex.unlock();
    	}
    }
    
    public static void unpauseConvergence() {
    	convergencePauseMutex.lock();
    	try {
    		if (convergencePaused) {
				Log.warning("Unpausing convergence");
    			convergencePaused = false;
    			convergencePauseCV.signalAll();
    		}
    	} finally {
        	convergencePauseMutex.unlock();
    	}
    }
    
    private static void blockWhilePaused() {
    	convergencePauseMutex.lock();
    	try {
    		while (convergencePaused) {
    			try {
    				convergencePauseCV.await();
    			} catch (InterruptedException ie) {
    			}
    		}
    	} finally {
        	convergencePauseMutex.unlock();
    	}
    }
    
    private static class PausedChecksumWorker implements Runnable {
    	public void run() {
    		while (true) {
    			try {
    				PausedChecksumTree	pct;

    				blockWhilePaused();
    				pct = pausedChecksumTrees.take();
    				Log.warning("Unpausing checksum tree ", pct.uuid);
    				_incomingChecksumTree(pct.uuid, pct.remoteTree, pct.cp, pct.connection);    						
    			} catch (Exception e) {
    				Log.logErrorWarning(e);
    			}
    		}
    	}
    }
    
    private static class PausedSignalWorker implements Runnable {
    	public void run() {
    		while (true) {
    			try {
    				ConvergenceController2	cc;

    				blockWhilePaused();
    				cc = pausedSignalledControllers.take();
    				Log.warning("Unpausing signalled convergence controller ", cc.getMyUUID());
    				cc.startConvergence();    						
    			} catch (Exception e) {
    				Log.logErrorWarning(e);
    			}
    		}
    	}
    }
    
    static {
    	new Thread(new PausedChecksumWorker(), "PausedChecksumWorker").start();
    	new Thread(new PausedSignalWorker(), "PausedSignalWorker").start();
    }
    
    private static class MQListener implements MultipleConnectionQueueLengthListener {
		@Override
		public void queueLength(UUIDBase uuid, int queueLength, Connection maxQueuedConnection) {
			if (convergencePaused || Log.levelMet(Level.INFO)) {
				Log.warning(String.format("Connections queue length:\t%d\t%s\t%d\n", queueLength, maxQueuedConnection, 
						(maxQueuedConnection != null ? maxQueuedConnection.getQueueLength() : 0)));
			}
			if (queueLength > queuePauseThreshold) {
				//pauseConvergence();
			} else if (queueLength < queueUnpauseThreshold) {
				//unpauseConvergence();
			}
		}    	
    }
    
    public static final MultipleConnectionQueueLengthListener	mqListener = new MQListener();
    public static final UUIDBase								mqUUID = new UUIDBase();
}

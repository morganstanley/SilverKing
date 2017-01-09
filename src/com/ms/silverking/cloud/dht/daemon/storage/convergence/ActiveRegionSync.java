package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.MapMaker;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.dht.daemon.storage.KeyedOpResultListener;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoRetrievalMessageGroup;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.LongInterval;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;

public class ActiveRegionSync implements KeyedOpResultListener {
	private final UUIDBase	uuid;
	private final NamespaceStore	nsStore;
	private final ChecksumTreeRequest	ctr;
	private final ChecksumTreeServer	checksumTreeServer;
	private final MessageGroupBase	mgBase;
	private final Lock		completionLock;
	private final Condition	completionCV;	
    private final Map<UUIDBase,SyncRetrievalRequest>	outstandingSyncRetrievalRequests;	
	private boolean	isComplete;    

	
	private static final boolean	debug = true;
	private static final boolean	verbose = true;
	
    private static final long   checksumVCMin = Long.MIN_VALUE + 1;    
    private static final int    convergenceRelativeDeadlineMillis = 10 * 60 * 1000;
    private static final long	checksumTreeRequestTimeout = 1 * 60 * 1000;
    private static final int    retrievalBatchSize = 256;
    private static final byte[] emptyUserData = new byte[0];
    
    private static final ConcurrentMap<UUIDBase,ActiveRegionSync>	activeRegionSyncs;	
    
    
    static {
    	activeRegionSyncs = new MapMaker().weakValues().makeMap();
    }
	
	public ActiveRegionSync(NamespaceStore nsStore, ChecksumTreeServer checksumTreeServer, MessageGroupBase mgBase, ChecksumTreeRequest ctr) {
		uuid = new UUIDBase(false);
		this.nsStore = nsStore;
		this.ctr = ctr;
		this.mgBase = mgBase;
		this.checksumTreeServer = checksumTreeServer;
		completionLock = new ReentrantLock();
		completionCV = completionLock.newCondition();
		outstandingSyncRetrievalRequests = new ConcurrentHashMap<>();	
	}
	
	public UUIDBase getUUID() {
		return uuid;
	}
	
	public void startSync() {
		sendChecksumTreeRequest(ctr);
	}    
	
	public void incomingChecksumTree(ConvergencePoint cp, ChecksumNode remoteTree, MessageGroupConnection connection) {
		ChecksumNode localTree;
		MatchResult matchResult;
		Set<DHTKey> keysToFetch;
		List<DHTKey> keysToFetchList;

		if (verbose) {
			System.out.printf("incomingChecksumTree %s %s %s\n", uuid, cp, connection.getRemoteIPAndPort());
		}
		if (remoteTree == null) {
			if (debug) {
				Log.warning("null incomingChecksumTree");
			}
			checkForCompletion();
			return;
		}
		localTree = checksumTreeServer.getRegionChecksumTree_Local(cp,
				remoteTree.getRegion(),
				new LongInterval(Long.MIN_VALUE, cp.getDataVersion()));
		if (debug) {
			System.out.printf("incomingChecksumTree %x\t%s\t%s\t%s\n", nsStore.getNamespace(), cp,
					remoteTree.getRegion(), uuid);
			System.out.println(remoteTree + "\n\nlocalTree\n" + localTree);
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
			System.out.println(matchResult);
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
		 * Queue<DHTKey> keysToFetchQueue;
		 * 
		 * keysToFetchQueue = keysToFetchMap.get(cp); if (keysToFetchQueue ==
		 * null) { Queue<DHTKey> prev;
		 * 
		 * keysToFetchQueue = new ConcurrentLinkedQueue<>(); prev =
		 * keysToFetchMap.putIfAbsent(cp, keysToFetchQueue); if (prev != null) {
		 * keysToFetchQueue = prev; } } keysToFetchQueue.addAll(keysToFetch);
		 */
		keysToFetchList = new LinkedList<>(keysToFetch);
		while (keysToFetchList.size() > 0) {
			Set<DHTKey> batchKeys;
			int batchSize;
			RetrievalOptions retrievalOptions;
			UUIDBase uuid;
			// MessageGroup mg;
			SyncRetrievalRequest srr;

			// batchKeys = new HashSet<>(retrievalBatchSize);
			batchKeys = new ConcurrentSkipListSet<DHTKey>();
			batchSize = 0;
			while (keysToFetchList.size() > 0 && batchSize < retrievalBatchSize) {
				batchKeys.add(keysToFetchList.remove(0));
				++batchSize;
			}

			// FUTURE - could consider sending SourceNotInDest
			retrievalOptions = OptionsHelper.newRetrievalOptions(
					RetrievalType.VALUE_AND_META_DATA, WaitMode.GET,
					checksumVersionConstraint(cp.getDataVersion()));
			// new VersionConstraint(Long.MIN_VALUE + 1, version, Mode.NEWEST));
			uuid = new UUIDBase(false);
			srr = new SyncRetrievalRequest(uuid, batchKeys, cp.getDataVersion(), connection);
			activeRegionSyncs.put(uuid, this);
			outstandingSyncRetrievalRequests.put(uuid, srr);
			sendSyncRetrievalRequest(srr);
		}
		//checkMGQueue();
		if (debug) {
			System.out.println("no more keysToFetch");
		}
		checkForCompletion();
	}
	
	////////////////////////////////////////////////////////////////////
	
    public void incomingSyncRetrievalResponse(MessageGroup message) {
        List<StorageValueAndParameters> svpList;
        SyncRetrievalRequest	srr;
        
        srr = outstandingSyncRetrievalRequests.get(message.getUUID());
        
        //outstandingMessages.decrementAndGet();
        //if (outstandingMessages.get() < 0) {
       //     outstandingMessages.set(0);
        //}
        
        if (debug) {
            Log.warning("incomingSyncRetrievalResponse");
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
        //checkMGQueue();
        checkForCompletion();
    }	
	
	////////////////////////////////////////////////////////////////////
	
    private void sendChecksumTreeRequest(ChecksumTreeRequest ctr) {
        MessageGroup    mg;
    	
        mg = new ProtoChecksumTreeRequestMessageGroup(uuid, nsStore.getNamespace(), ctr.getTargetCP(), ctr.getCurCP(),  
                                                      mgBase.getMyID(), ctr.getRegion(), false).toMessageGroup(); 
        if (verbose || debug) {
            System.out.printf("%x requestChecksumTree: %s\t%s\t%s\t%s\n", nsStore.getNamespace(), ctr.getReplica(), ctr.getRegion(), ctr.getTargetCP(), ctr.getCurCP());
        }
        mgBase.send(mg, ctr.getReplica());
        ctr.setSent();
    }
	
    private static VersionConstraint checksumVersionConstraint(long max) {
        return new VersionConstraint(checksumVCMin, Long.MAX_VALUE, VersionConstraint.Mode.GREATEST);
        //return new VersionConstraint(checksumVCMin, max, VersionConstraint.Mode.NEWEST);
        //return VersionConstraint.newest;
    }
	
    private void sendSyncRetrievalRequest(SyncRetrievalRequest srr) {
        MessageGroup		mg;
        RetrievalOptions    retrievalOptions;
        
        retrievalOptions = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE_AND_META_DATA, WaitMode.GET,
                checksumVersionConstraint(srr.dataVersion));
        mg = new ProtoRetrievalMessageGroup(srr.uuid, nsStore.getNamespace(), new InternalRetrievalOptions(retrievalOptions),
                mgBase.getMyID(), srr.outstandingKeys, convergenceRelativeDeadlineMillis).toMessageGroup();
        //outgoingMessages.add(new OutgoingMessage(mg, new IPAndPort(srr.connection.getRemoteSocketAddress())));
        mgBase.send(mg, srr.connection.getRemoteIPAndPort());
    }
    
    //KeyedOpResultListener implementation; only used for ignoring results of puts for now 
    public void sendResult(DHTKey key, OpResult result) {
    }
    
    //////////////////////////////////////////////////////////////////////
    // completion check
    
    private void checkForCompletion() {
        if (outstandingSyncRetrievalRequests.isEmpty()) {
        	setComplete();
        }
    }    
	
	////////////////////////////////////////////////////////////////////
	
	private void setComplete() {
		completionLock.lock();
		try {
			if (debug) {
				Log.warning("setComplete");
			}
			isComplete = true;
			completionCV.signalAll();
		} finally {
			completionLock.unlock();
		}
	}
	
	public boolean waitForCompletion(long time, TimeUnit unit) {
		Timer	timer;
		
		timer = new SimpleTimer(unit, time);
		completionLock.lock();
		try {
			while (!isComplete) {
				boolean	timedOut;
				
				try {
					if (debug) {
						Log.warningf("ars completionCV.await %s", uuid);
					}
					timedOut = !timer.await(completionCV);
					if (debug) {
						Log.warningf("ars completionCV await done %s", uuid);
					}
				} catch (InterruptedException e) {
					timedOut = false;
				}
				if (timedOut) {
					return false;
				}
			}
		} finally {
			completionLock.unlock();
		}
		return true;
	}
	
	
	//////////////////////////////////////////////////////////////////////////////////////
	
    protected static AbsMillisTimeSource  absMillisTimeSource;
    
    public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
        absMillisTimeSource = _absMillisTimeSource;
    }	
	
    private static class SyncRetrievalRequest {
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
    
    public static boolean _incomingSyncRetrievalResponse(MessageGroup message) {
        ActiveRegionSync	ars;
        
        ars = activeRegionSyncs.get(message.getUUID());
        if (ars != null) {
        	ars.incomingSyncRetrievalResponse(message);
            return true;
        } else {
            Log.info("No ActiveRegionSync for uuid: ", message.getUUID());
            return false;
        }
    }
}

package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.collection.HashedListMap;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Stopwatch;
import com.ms.silverking.time.Timer;

class SyncController {
	private final MessageGroupBase		mgBase;
	private final AbsMillisTimeSource	absMillisTimeSource;
	private final ConvergencePoint		curCP;
	private final ConvergencePoint		targetCP;
	private final Set<ReplicaSyncRequest>						queuedSyncs;
	private final HashedListMap<IPAndPort,ReplicaSyncRequest>	ownerToActiveSyncs;
	private final Map<UUIDBase,ReplicaSyncRequest>				activeSyncs;
	private final Lock	lock;
	private boolean	frozen;
	private int	totalRequests;
	private volatile int	queuedSyncsSize;
	private volatile int	activeSyncsSize;
	private boolean	abandoned;
	private boolean	hasErrors;
	private final BlockingQueue<Pair<UUIDBase,OpResult>>	completionQueue;
	
	private static final int	completionQueueCapacity = 10000;
	
	private static final double	resendIntervalSeconds = 1.0 * 60.0;
	private static final double	nonCompletionAlertThresholdSeconds = 5.0;
	
	private static final boolean	verbose = true;
	private static final boolean	debug = false;
	
	private static final int	maxConcurrentNewOwnerRequests = 12;
	private static final int	maxConcurrentOldOwnerRequests = 4;
	
	static {
		if (maxConcurrentNewOwnerRequests + maxConcurrentOldOwnerRequests > StorageModule.methodCallPoolMaxSize) {
			// If this constraint is violated, the convergence may deadlock
			throw new RuntimeException("maxConcurrentNewOwnerRequests + maxConcurrentOldOwnerRequests > StorageModule.methodCallPoolMaxSize");
		}
	}

	SyncController(MessageGroupBase mgBase, ConvergencePoint curCP, ConvergencePoint targetCP, AbsMillisTimeSource absMillisTimeSource) {
		this.mgBase = mgBase;
		this.curCP = curCP;
		this.targetCP = targetCP;
		this.absMillisTimeSource = absMillisTimeSource;
		ownerToActiveSyncs = new HashedListMap<>();
		activeSyncs = new HashMap<>();
		queuedSyncs = new HashSet<>();
		lock = new ReentrantLock();
		completionQueue = new ArrayBlockingQueue<>(completionQueueCapacity);
	}

	// unlocked, requests must be queued before they become active
	void queueRequest(ReplicaSyncRequest r) {
		ensureNotFrozen();
		queuedSyncs.add(r);
	}
	
	void freeze() {
		lock.lock();
		try {
			ensureNotFrozen();
			frozen = true;
			totalRequests = queuedSyncs.size();
		} finally {
			lock.unlock();
		}
	}
	
	private void ensureFrozen() {
		if (!frozen) {
			throw new RuntimeException("ensureFrozen failed()");
		}
	}
	
	private void ensureNotFrozen() {
		if (frozen) {
			throw new RuntimeException("ensureNotFrozen failed()");
		}
	}
	
	private boolean activeRequestsAboveLimit(IPAndPort owner, int limit) {
		return ownerToActiveSyncs.listSize(owner) >= limit;
	}
	
	private boolean isActive(IPAndPort owner) {
		return activeRequestsAboveLimit(owner, 1);
	}
	
	private void setActive(ReplicaSyncRequest r) {
		ownerToActiveSyncs.addValue(r.getNewOwner(), r);
		ownerToActiveSyncs.addValue(r.getOldOwner(), r);
		activeSyncs.put(r.getUUID(), r);
	}
	
	private void setInactive(ReplicaSyncRequest r) {
		ownerToActiveSyncs.removeValue(r.getNewOwner(), r);
		ownerToActiveSyncs.removeValue(r.getOldOwner(), r);
		activeSyncs.remove(r.getUUID());
	}
	
	void requestComplete(UUIDBase uuid, OpResult opResult) {
		try {
			completionQueue.put(new Pair<>(uuid, opResult));
		} catch (InterruptedException e) {
			throw new RuntimeException("Unexpected interruption");
		}
	}
	
	boolean serviceCompletionQueue(long timeout, TimeUnit unit) {
		int	numCompletions;
		Set<Pair<UUIDBase,OpResult>>	completions;
		
		completions = new HashSet<>();
		numCompletions = completionQueue.drainTo(completions);
		if (numCompletions > 0) {
			for (Pair<UUIDBase,OpResult> completion : completions) {
				_requestComplete(completion.getV1(), completion.getV2());
			}
			return true;
		} else {
			return false;
		}
	}
	
	private boolean _requestComplete(UUIDBase uuid, OpResult opResult) {
		boolean	found;
		
		ReplicaSyncRequest	r;
		
		if (opResult.hasFailed()) {
			hasErrors = true;
			abandon();
			Log.warningAsyncf("SyncController request failed: %s", uuid);
		}
		r = activeSyncs.get(uuid);
		if (r == null) {
			found = false;
			Log.warningAsyncf("Couldn't find any active request for %s", uuid);
		} else {
			found = true;
			Log.warningAsyncf("Setting inactive %s", uuid);
			setInactive(r);
		}
		return found;
	}	

	void sendNonConflictingRequests() {
		List<ReplicaSyncRequest>	syncsToSend;
		
		syncsToSend = new ArrayList<>();
		for (ReplicaSyncRequest r : queuedSyncs) {
			if (!activeRequestsAboveLimit(r.getNewOwner(), maxConcurrentNewOwnerRequests) && !activeRequestsAboveLimit(r.getOldOwner(), maxConcurrentOldOwnerRequests)) {
				syncsToSend.add(r);
				setActive(r);
				sendReplicaSyncRequest(r);
			}
		}
		queuedSyncs.removeAll(syncsToSend);
	}
	
	void resendActiveRequests() {
    	Log.warningAsyncf("Resending active requests");
		for (ReplicaSyncRequest r : activeSyncs.values()) {
			sendReplicaSyncRequest(r);
		}
	}
	
    private void sendReplicaSyncRequest(ReplicaSyncRequest r) {
        MessageGroup	mg;
    	
        ensureFrozen();
        mg = new ProtoChecksumTreeRequestMessageGroup(r.getUUID(), r.getNS(), targetCP, curCP,  
                                                      mgBase.getMyID(), r.getRegion(), r.getOldOwner(), true).toMessageGroup(); 
        if (verbose || debug) {
            Log.warningAsyncf("%x requestChecksumTree: %s", r.getNS(), r);
        }
        if (!hasErrors) {
	        mgBase.send(mg, r.getNewOwner());
	        r.setSendTime(absMillisTimeSource.absTimeMillis());
        }
    }
    
    private boolean complete() {
    	return (activeSyncs.isEmpty() && queuedSyncs.isEmpty()) || hasErrors || abandoned;
    }
    
    public void waitForCompletion(long time, TimeUnit unit) throws ConvergenceException {
    	Timer	timer;
    	
    	timer = new SimpleTimer(unit, time);
		lock.lock();
    	try {
			boolean	nonZeroCompletions;
			Stopwatch	resendSW;
			Stopwatch	alertSW;
			boolean		incompleteDisplayed;
			
			incompleteDisplayed = false;
			nonZeroCompletions = false;
			alertSW = new SimpleStopwatch();
			resendSW = new SimpleStopwatch();
    		while (!complete() && !timer.hasExpired()) {
    			if (!nonZeroCompletions) {
    				ThreadUtil.sleep(1);
    			}
    			computeStatus();
    			sendNonConflictingRequests();
    			nonZeroCompletions = serviceCompletionQueue(timer.getRemainingMillisLong(), TimeUnit.MILLISECONDS);
    			if (nonZeroCompletions) {
    				incompleteDisplayed = false;
    				alertSW.reset();
    				resendSW.reset();
    			} else {
    				if (alertSW.getSplitSeconds() > nonCompletionAlertThresholdSeconds && !incompleteDisplayed) {
    					incompleteDisplayed = true;
    					displayIncomplete();
    				}
    				if (resendSW.getSplitSeconds() > resendIntervalSeconds) {
    					resendActiveRequests();
    					resendSW.reset();
    				}
    			}
    		}
    		if (hasErrors) {
    			throw new ConvergenceException("SyncController unable to complete due to errors");
    		} else if (abandoned) {
    			throw new ConvergenceException("Abandoned");
    		} else if (!complete()) {
    			throw new ConvergenceException("Sync timed out");
    		}
    	} finally {
    		lock.unlock();
    	}
    }

    private void displayIncomplete() {
    	Log.warningAsync("Incomplete requests");
    	for (Map.Entry<UUIDBase, ReplicaSyncRequest> e : activeSyncs.entrySet()) {
    		Log.warningAsyncf("%s\t%s", e.getKey(), e.getValue());
    	}
    	Log.warningAsync("End incomplete requests");
	}

	private void computeStatus() {
    	queuedSyncsSize = queuedSyncs.size();
    	activeSyncsSize = activeSyncs.size();
    }

	// lock must be held
    public void abandon() {
		lock.lock();
    	try {
       		queuedSyncs.clear();
       		abandoned = true;
    	} finally {
    		lock.unlock();
    	}
    }
    
    public Triple<Integer,Integer,Integer> getStatus() {
		return new Triple<>(queuedSyncsSize, activeSyncsSize, totalRequests);
    }
 }

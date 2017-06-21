package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ConvergencePoint;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.collection.HashedListMap;
import com.ms.silverking.collection.Pair;
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
	private final Set<Action>									ineligibleActions;
	private final Set<ReplicaSyncRequest>						eligibleSyncs; // requests with all dependencies satisfied
	private final ConcurrentMap<IPAndPort,Set<ReplicaSyncRequest>>	ownerToActiveSyncs;
	private final ConcurrentMap<UUIDBase,ReplicaSyncRequest>	activeSyncs;
	private final ConcurrentMap<UUIDBase,Integer>				outstandingSyncRetrievalRequests;
	private final Set<UUIDBase>									completeActions;
	private final AtomicInteger									completeActions_size;
	private final AtomicInteger									recentCompletions;
	private final Lock	lock;
	private boolean	frozen;
	private int	totalActions;
	private volatile int	ineligibleActionsSize;
	private volatile int	eligibleSyncsSize;
	private volatile int	activeSyncsSize;
	private volatile long	totalOutstandingSyncRetrievalRequests;
	private boolean	abandoned;
	private boolean	hasErrors;
	private final BlockingQueue<Pair<UUIDBase,OpResult>>	completionQueue;
	private final Stopwatch	elapsedSW;
	
	private static final int	completionQueueCapacity = 10000;
	
	private static final double	resendCheckIntervalSeconds = 15.0;
	private static final double	resendIntervalSeconds = 2.0 * 60.0 * 60.0;
	private static final double	nonCompletionAlertThresholdSeconds = 5.0;
	
	private static final int		statusComputationIntervalMillis = 400;
	private static final TimeUnit	statusComputationIntervalUnit = TimeUnit.MILLISECONDS;
	
	private static final int	syncsToSendInitialSize = 100;
	
	private static final boolean	verbose = true;
	private static final boolean	debug = false;
	
	private static final int	maxConcurrentNewOwnerRequests = 8;
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
		ownerToActiveSyncs = new ConcurrentHashMap<>();
		activeSyncs = new ConcurrentHashMap<>();
		outstandingSyncRetrievalRequests = new ConcurrentHashMap<>();
		ineligibleActions = new ConcurrentSkipListSet<>();
		eligibleSyncs = new ConcurrentSkipListSet<>();
		lock = new ReentrantLock();
		completionQueue = new ArrayBlockingQueue<>(completionQueueCapacity);
		completeActions = new ConcurrentSkipListSet<>();
		completeActions_size = new AtomicInteger();
		recentCompletions = new AtomicInteger();
		elapsedSW = new SimpleStopwatch();
	}

	// unlocked, requests must be queued before they become active
	void addAction(Action a) {
		ensureNotFrozen();
		ineligibleActions.add(a);
	}
	
	void addCompleteAction(Action a) {
		ensureNotFrozen();
		completeActions.add(a.getUUID());
	}
	
	void freeze() {
		lock.lock();
		try {
			ensureNotFrozen();
			frozen = true;
			totalActions = ineligibleActions.size();
			Log.warning("totalActions: ", totalActions);
			computeDownstreamDependencies();
		} finally {
			lock.unlock();
		}
	}
	
	private void computeDownstreamDependencies() {
		HashedListMap<Action,Action>	downstreamDepencencies;
		
		downstreamDepencencies = new HashedListMap<>();
		for (Action a : ineligibleActions) {
			for (Action ua : a.getUpstreamDependencies()) {
				downstreamDepencencies.addValue(ua, a);
			}
		}
		for (Action a : downstreamDepencencies.getKeys()) {
			List<Action>	dependencies;
			
			dependencies = downstreamDepencencies.getList(a);
			a.addDownstreamDependencies(dependencies.toArray(new Action[0]));
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
		Set<ReplicaSyncRequest>	s;
		
		s = ownerToActiveSyncs.get(owner);
		if (s == null) {
			Log.warning("Couldn't find sync set for: ", owner);
			return false;
		} else {
			return s.size() >= limit;
		}
	}
	
	private boolean isActive(IPAndPort owner) {
		return activeRequestsAboveLimit(owner, 1);
	}
	
	private void addSync(IPAndPort owner, ReplicaSyncRequest r) {
		Set<ReplicaSyncRequest>	s;
		
		s = ownerToActiveSyncs.get(owner);
		if (s == null) {
			ownerToActiveSyncs.putIfAbsent(owner, new ConcurrentSkipListSet<>());
			s = ownerToActiveSyncs.get(owner);
		}
		s.add(r);
	}
	
	private void removeSync(IPAndPort owner, ReplicaSyncRequest r) {
		Set<ReplicaSyncRequest>	s;
		
		s = ownerToActiveSyncs.get(owner);
		if (s != null) {
			s.remove(r);
		} else {
			Log.warningf("Couldn't find for removal %s owner %s", r, owner);
		}
	}
	
	private void setActive(ReplicaSyncRequest r) {
		addSync(r.getNewOwner(), r);
		addSync(r.getOldOwner(), r);
		activeSyncs.put(r.getUUID(), r);
	}
	
	private void setInactive(ReplicaSyncRequest r) {
		if (activeSyncs.remove(r.getUUID()) != null) {
			removeSync(r.getNewOwner(), r);
			removeSync(r.getOldOwner(), r);
			if (completeActions.add(r.getUUID())) {
				completeActions_size.incrementAndGet();
			}
			checkDownstreamDependencies(r);
		}
		outstandingSyncRetrievalRequests.remove(r.getUUID());
	}
	
	private void checkDownstreamDependencies(Action a) {
		if (Log.levelMet(Level.INFO)) {
			Log.warningf("checkDownstreamDependencies %s", a);
		}
		for (Action d : a.getDownstreamDependencies()) {
			checkUpstreamDependencies(d);
		}
	}
	
	private void checkUpstreamDependencies(Action a) {
		if (Log.levelMet(Level.INFO)) {
			Log.warningf("\tcheckUpstreamDependencies %s", a);
		}
		// Note we could be in here concurrently
		if (ineligibleActions.contains(a)) {
			boolean	removed;
			Action	lastIncomplete;
			
			lastIncomplete = a.getLastIncompleteUpstream();
			if (lastIncomplete != null) {
				if (!completeActions.contains(lastIncomplete.getUUID())) {
					if (Log.levelMet(Level.INFO)) {
						Log.warningf("\t\t!complete %s", lastIncomplete);
					}
					return; // a has an upstream dependency that has not been satisfied
				}
			}
			for (Action u : a.getUpstreamDependencies()) {
				if (!completeActions.contains(u.getUUID())) {
					if (Log.levelMet(Level.INFO)) {
						Log.warningf("\t\t!complete %s", u);
					}
					a.setLastIncompleteUpstream(u);
					return; // a has an upstream dependency that has not been satisfied
				}
			}
			// all upstream dependencies of a have been satisfied
			removed = ineligibleActions.remove(a);
			if (removed) {
				if (Log.levelMet(Level.INFO)) {
					Log.warningf("\t\tnewly eligible %s", a);
				}
				if (a instanceof ReplicaSyncRequest) {
					ReplicaSyncRequest	r;
					
					r = (ReplicaSyncRequest)a;
					eligibleSyncs.add(r);
				} else {
					SynchronizationPoint	sp;
					
					sp = (SynchronizationPoint)a;
					if (completeActions.add(sp.getUUID())) {
						completeActions_size.incrementAndGet();
					}
					checkDownstreamDependencies(sp);
				}
			}
		}
	}

	// begin incomplete implementation of updates
	void requestUpdate(UUIDBase uuid, OpResult opResult) {
		if (opResult.isComplete()) {
			requestComplete(uuid, opResult);
		} else {
			_requestInProgress(uuid);
		}
	}
	
	private void _requestInProgress(UUIDBase uuid) {
		ReplicaSyncRequest	r;
		
		r = activeSyncs.get(uuid);
		if (r != null) {
		}
	}
	// end incomplete implementation of updates

	void requestComplete(UUIDBase uuid, OpResult opResult) {
		recentCompletions.incrementAndGet();
		_requestComplete(uuid, opResult);
		/*
		try {
			completionQueue.put(new Pair<>(uuid, opResult));
		} catch (InterruptedException e) {
			throw new RuntimeException("Unexpected interruption");
		}
		*/
	}
	
	boolean serviceCompletionQueue(long timeout, TimeUnit unit) {
		/*
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
		*/
		return recentCompletions.getAndSet(0) != 0;
	}
	
	public boolean isInactive(UUIDBase uuid) {
		return completeActions.contains(uuid);
	}
	
	private boolean _requestComplete(UUIDBase uuid, OpResult opResult) {
		if (completeActions.contains(uuid)) {
			Log.warningAsyncf("Already complete. Ignoring completion: %s", uuid);
			return false;
		} else {
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
	}	
	
	void scanForEligibleActions() {
		for (Action a : ineligibleActions) {
			checkUpstreamDependencies(a);
		}
	}

	void sendNonConflictingRequests() {
		List<ReplicaSyncRequest>	syncsToSend;
		
		syncsToSend = new ArrayList<>(syncsToSendInitialSize);
		for (ReplicaSyncRequest r : eligibleSyncs) {
			if (!activeRequestsAboveLimit(r.getNewOwner(), maxConcurrentNewOwnerRequests) && !activeRequestsAboveLimit(r.getOldOwner(), maxConcurrentOldOwnerRequests)) {
				syncsToSend.add(r);
				setActive(r);
				sendReplicaSyncRequest(r);
			}
		}
		eligibleSyncs.removeAll(syncsToSend);
	}
	
	void checkForResends() {
		for (ReplicaSyncRequest r : activeSyncs.values()) {
			if (r.getSendTime() > 0 && (absMillisTimeSource.absTimeMillis() - r.getSendTime() > (long)(resendIntervalSeconds * 1000.0))) {
		        if (verbose || debug) {
		            Log.warningAsyncf("%x resending: %s %d", r.getNS(), r, absMillisTimeSource.absTimeMillis() - r.getSendTime());
		        }
				sendReplicaSyncRequest(r);
			}
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
    	return (completeActions_size.get() == totalActions) || hasErrors || abandoned;
    }
    
    public void waitForCompletion(long time, TimeUnit unit) throws ConvergenceException {
    	Timer	timer;
    	Timer	statusTimer;
    	
    	scanForEligibleActions();
    	timer = new SimpleTimer(unit, time);
    	statusTimer = new SimpleTimer(statusComputationIntervalUnit, statusComputationIntervalMillis);
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
    			if (statusTimer.hasExpired()) {
    				computeStatus();
    				statusTimer.reset();
    			}
    			sendNonConflictingRequests();
    			nonZeroCompletions = serviceCompletionQueue(timer.getRemainingMillisLong(), TimeUnit.MILLISECONDS);
    			if (nonZeroCompletions) {
    				incompleteDisplayed = false;
    				alertSW.reset();
    			} else {
    				if (alertSW.getSplitSeconds() > nonCompletionAlertThresholdSeconds && !incompleteDisplayed) {
    					incompleteDisplayed = true;
    					displayIncomplete();
    				}
    			}
    			if (resendSW.getSplitSeconds() > resendCheckIntervalSeconds) {
    				checkForResends();
    				resendSW.reset();
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
    
    private long computeTotalOutstandingSyncRetrievalRequests() {
    	long	total;
    	
    	total = 0;
    	for (Integer x : outstandingSyncRetrievalRequests.values()) {
    		total += x;
    	}
    	return total;
    }

	private void computeStatus() {
		ineligibleActionsSize = ineligibleActions.size();
    	eligibleSyncsSize = eligibleSyncs.size();
    	activeSyncsSize = activeSyncs.size();
    	totalOutstandingSyncRetrievalRequests = computeTotalOutstandingSyncRetrievalRequests();
    }

	// lock must be held
    public void abandon() {
		lock.lock();
    	try {
       		eligibleSyncs.clear();
       		abandoned = true;
    	} finally {
    		lock.unlock();
    	}
    }
    
    public double elapsedSeconds() {
    	return elapsedSW.getSplitSeconds();
    }
    
    public String getStatus() {
		return String.format("%s:%s:%s:%s:%s", ineligibleActionsSize, eligibleSyncsSize, activeSyncsSize, totalOutstandingSyncRetrievalRequests, ineligibleActionsSize + eligibleSyncsSize);
    }

	public void updateProgress(UUIDBase uuid, Pair<Long, Long> progress) {
		outstandingSyncRetrievalRequests.put(uuid, progress.getV1().intValue());
	}
 }

package com.ms.silverking.cloud.dht.meta;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.daemon.DaemonState;
import com.ms.silverking.cloud.zookeeper.ZooKeeperExtended;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.numeric.NumConversion;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class DaemonStateZK implements Watcher {
    private final MetaClient    mc;
    private final boolean		monitorOnly;
    private final IPAndPort     myIPAndPort;
    private final String        instanceDaemonStatePath;
    private final Lock			lock;
    private final Condition		cv;
    private DaemonState         state;
    private volatile int	expectedSignals; // a loose hint; ok if not strictly consistent
    
    private static final int    minQuorumStatePollIntervalMillis = 5 * 1000;
    private static final int    maxQuorumStatePollIntervalMillis = 20 * 1000;
    private static final int	maxQuorumStatePollThreshold = 500;
    private static final int	maxIncompletePerFetch = 50;
   private static final int    stateCheckPeriodMillis = 10 * 1000;
    
    private static final boolean    verbose = true;
    
    /*
     * Create a DaemonStateZK instance for maintaining a nodes state
     * and reading the state of other nodes. Intended for use by the DHTNode.
     */
    private DaemonStateZK(MetaClient mc, IPAndPort myIPAndPort, Timer timer, boolean monitorOnly) {
        this.mc = mc;
        this.myIPAndPort = myIPAndPort;
        this.monitorOnly = monitorOnly;
        instanceDaemonStatePath = mc.getMetaPaths().getInstanceDaemonStatePath();
        if (!monitorOnly) {
            setState(DaemonState.INITIAL);
            timer.scheduleAtFixedRate(new StateChecker(), 
                    ThreadLocalRandom.current().nextInt(stateCheckPeriodMillis), 
                    stateCheckPeriodMillis);
        }
        lock = new ReentrantLock();
        cv = lock.newCondition();
    }
    
    public DaemonStateZK(MetaClient mc, IPAndPort myIPAndPort, Timer timer) {
    	this(mc, myIPAndPort, timer, false);
    }
    
    /*
     * Create a DaemonStateZK instance for reading state only. 
     * Intended for use by the HealthMonitor.
     */
    public DaemonStateZK(MetaClient mc) {
    	this(mc, null, null, true);
    }    
    
    private String getMemberDaemonStatePath(IPAndPort member) {
        return instanceDaemonStatePath +"/"+ member;
    }
    
    public void setState(DaemonState state) {
    	if (monitorOnly) {
    		throw new RuntimeException("Can't setState using monitorOnly DaemonStateZK");
    	}
        if (verbose) {
            System.out.printf("Local DHTNode state: %s\n", state);
        }
        this.state = state;
        ensureStateSet();
    }
    
    public void ensureStateSet() {
    	if (monitorOnly) {
    		throw new RuntimeException("Can't ensureStateSet using monitorOnly DaemonStateZK");
    	}
        try {
            mc.getZooKeeper().setEphemeralInteger(getMemberDaemonStatePath(myIPAndPort), state.ordinal());
        } catch (KeeperException ke) {
            Log.logErrorWarning(ke);
        }
    }
    
    @Override
    public void process(WatchedEvent event) {
    	Log.fine(event);
        //if (mc.getZooKeeper().getState() == States.CONNECTED) {
        //    ensureStateSet();
        //}
    	switch (event.getType()) {
    	case None:
    		if (event.getState() == KeeperState.SyncConnected) {
    			ensureStateSet();
    		}
    		break;
    	case NodeCreated:
    		checkForSignal();
    		break;
    	case NodeDeleted:
    		break;
    	case NodeDataChanged:
    		checkForSignal();
    		break;
    	case NodeChildrenChanged:
    		checkForSignal();
    		break;
    	default:
    		Log.warning("Unknown event type: ", event.getType());
    	}
    }
    
    private void checkForSignal() {
    	if (--expectedSignals <= 0) {
    		signalAwait();
    	}
    }
    
    public Set<IPAndPort> readActiveNodesFromZK() throws KeeperException {
        String              basePath;
        ZooKeeperExtended   _zk;
        
        basePath = mc.getMetaPaths().getInstanceDaemonStatePath();
        _zk = mc.getZooKeeper();
        return ImmutableSet.copyOf(IPAndPort.list(_zk.getChildren(basePath, this)));
    }
    
    private Map<IPAndPort, DaemonState> _getQuorumState(Set<IPAndPort> members) throws KeeperException {
    	ZooKeeperExtended	zk;
    	Set<String>			daemonStatePaths;
    	Map<String,Integer>	daemonStates;
        Map<IPAndPort, DaemonState> quorumState;
        
    	daemonStatePaths = new HashSet<>(members.size());
    	for (IPAndPort member : members) {
    		daemonStatePaths.add(getMemberDaemonStatePath(member));
    	}
    	zk = mc.getZooKeeper();
    	daemonStates = zk.getInts(daemonStatePaths, this);
    	
        quorumState = new HashMap<>();
    	for (IPAndPort member : members) {
    		Integer		rawState;
    		
    		rawState = daemonStates.get(getMemberDaemonStatePath(member));
    		if (rawState != null) {
        		DaemonState	ds;
        		
    			ds = DaemonState.values()[rawState];
    			quorumState.put(member, ds);
    		}
    	}    	
    	return quorumState;
    }
    
    public DaemonState getMemberState(IPAndPort member) throws KeeperException {
    	try {
	    	byte[]	data;
	    	
	    	data = mc.getZooKeeper().getByteArray(getMemberDaemonStatePath(member), this);
            return DaemonState.values()[NumConversion.bytesToInt(data)];
    	} catch (NoNodeException nne) {
    		return null;
    	}
    }
    
    public DaemonState _getMemberState(IPAndPort member) throws KeeperException {
        if (mc.getZooKeeper().exists(getMemberDaemonStatePath(member))) {
            return DaemonState.values()[mc.getZooKeeper().getInt(getMemberDaemonStatePath(member), this)];
        } else {
            return null;
        }
    }
    
    public Pair<Boolean,Map<IPAndPort, DaemonState>> getQuorumState(Set<IPAndPort> members, DaemonState targetState) throws KeeperException {
    	return getQuorumState(members, targetState, maxIncompletePerFetch);
    }
    
    public Pair<Boolean,Map<IPAndPort, DaemonState>> getAllQuorumState(Set<IPAndPort> members, DaemonState targetState) throws KeeperException {
    	return getQuorumState(members, targetState, Integer.MAX_VALUE);
    }
    
    private Pair<Boolean,Map<IPAndPort, DaemonState>> getQuorumState(Set<IPAndPort> members, DaemonState targetState, int _maxIncompletePerFetch) throws KeeperException {
        Map<IPAndPort, DaemonState> quorumState;
        boolean	allRead;
        int		incomplete;
        
        quorumState = new HashMap<>();
        allRead = true;
        incomplete = 0;
        for (IPAndPort member : members) {
            DaemonState state;
            
            state = getMemberState(member);
            Log.warningf("member %s state %s", member, state);
    		quorumState.put(member, state);
        	if (state == null || state.ordinal() < targetState.ordinal()) {
        		++incomplete;
            	if (incomplete >= _maxIncompletePerFetch && incomplete < members.size()) {
            		allRead = false;
            		break;
            	}
        	}
        }
        return new Pair<>(allRead, quorumState);
    }
    
    public boolean waitForQuorumState(Set<IPAndPort> members, DaemonState targetState, int inactiveNodeTimeoutSeconds) {
    	return waitForQuorumState(members, targetState, inactiveNodeTimeoutSeconds, false).isEmpty();
    }
    
    public Map<IPAndPort, DaemonState> waitForQuorumState(Set<IPAndPort> members, DaemonState targetState, int inactiveNodeTimeoutSeconds, boolean exitOnTimeout) {
        boolean targetReached;
        int     lastNotReadyCount;
        int     notReadyCount;
        int     inactiveCount;
        Stopwatch   sw;
        Set<IPAndPort>	incompleteMembers;

        incompleteMembers = new HashSet<>(members);
        if (verbose) {
            System.out.printf("Waiting for quorum state: %s\n", targetState);
        }
        sw = new SimpleStopwatch();
        lastNotReadyCount = Integer.MAX_VALUE;
        targetReached = false;
        randomAwait(0, minQuorumStatePollIntervalMillis);
        while (!targetReached) {
            StringBuilder   sb;
            
            sb = new StringBuilder();
            try {
	            if (mc.getZooKeeper().getState() == States.CONNECTED) {
                    Map<IPAndPort, DaemonState> quorumState;
                    Stopwatch	qsSW;
                    Set<IPAndPort>	newlyCompleteMembers;
                	Pair<Boolean,Map<IPAndPort, DaemonState>>	rVal;
                	boolean	allRead;

                    qsSW = new SimpleStopwatch();
                    rVal = getQuorumState(incompleteMembers, targetState);
                    qsSW.stop();
                    Log.info("getQuorumState elapsed ", qsSW.getElapsedSeconds());
                    allRead = rVal.getV1();
                    quorumState = rVal.getV2();
                    targetReached = true;
                    notReadyCount = 0;
                    inactiveCount = 0;
                    newlyCompleteMembers = new HashSet<>();
                    for (IPAndPort incompleteMember : incompleteMembers) {
                        DaemonState memberState;
                        
                        memberState = quorumState.get(incompleteMember);
                        if (verbose && allRead) {
                            sb.append(String.format("%s\t%s\n", incompleteMember, memberState));
                        }
                        if (memberState == null) {
                        	if (allRead) {
	                            ++inactiveCount;
	                            if ((int)sw.getSplitSeconds() < inactiveNodeTimeoutSeconds) {
	                                ++notReadyCount;
	                                targetReached = false;
	                            } else {
	                                sb.append(String.format("%s\t%s\tinactive node timed out\n", incompleteMember, memberState));
	                                if (exitOnTimeout) {
	                                	Map<IPAndPort, DaemonState>	stateMap;
	                                	
	    	                            System.out.print(sb);
	                                	Log.warningf("Timeout: %s", incompleteMember);
	                                	stateMap = fetchAndDisplayIncompleteState(incompleteMembers, targetState);
	                                	return stateMap;
	                                }
	                            }
                        	} else {
                        		targetReached = false;
                        	}
                        } else if (memberState.ordinal() < targetState.ordinal()) {
                            targetReached = false;
                            ++notReadyCount;
                        } else {
                        	newlyCompleteMembers.add(incompleteMember);
                        }
                    }
                	incompleteMembers.removeAll(newlyCompleteMembers);
                    if (verbose) {
                    	if (allRead) {
	                        if (notReadyCount != lastNotReadyCount) {
	                            System.out.print(sb);
	                        } else {
	                            System.out.printf("Waiting for %d nodes\n", notReadyCount);
	                        }
                    	} else {
                            System.out.printf("waitForQuorumState > %d incomplete\n", maxIncompletePerFetch);
                    	}
                    }
                    lastNotReadyCount = notReadyCount;
	            }
            } catch (KeeperException ke) {
                Log.logErrorWarning(ke);
            }
            if (!targetReached) {
            	int	maxSleepMillis;
            	
                if ((int)sw.getSplitSeconds() > inactiveNodeTimeoutSeconds) {
                    if (exitOnTimeout) {
                    	Map<IPAndPort, DaemonState>	stateMap;
                    	
                    	Log.warningf("Timeout in targetState: %s", targetState);
                    	try {
							stateMap = fetchAndDisplayIncompleteState(incompleteMembers, targetState);
						} catch (KeeperException e) {
							e.printStackTrace();
							stateMap = ImmutableMap.of();
						}
                    	return stateMap;
                    }
                }
            	
            	maxSleepMillis = (int)Math.max((double)maxQuorumStatePollIntervalMillis * ((double)incompleteMembers.size()/(double)maxQuorumStatePollThreshold), minQuorumStatePollIntervalMillis);
            	maxSleepMillis = Math.min(maxSleepMillis, maxQuorumStatePollIntervalMillis);
            	Log.info("waitForQuorumState maxSleepMillis: ", maxSleepMillis);
            	expectedSignals = incompleteMembers.size();
            	randomAwait(minQuorumStatePollIntervalMillis, maxSleepMillis);
            	Log.info("waitForQuorumState awake");
            }
        }
        if (verbose) {
            System.out.printf("Quorum state reached: %s\n", targetState);
        }
        return ImmutableMap.of();
    }
    
    private Map<IPAndPort,DaemonState> fetchAndDisplayIncompleteState(Set<IPAndPort> members, DaemonState targetState) throws KeeperException {
    	Pair<Boolean,Map<IPAndPort, DaemonState>>	sp;
    	Map<IPAndPort, DaemonState>					qs;
    	
    	sp = getAllQuorumState(members, targetState);
    	qs = sp.getV2();
        for (IPAndPort im : members) {
        	DaemonState	state;
        	
        	state = qs.get(im);
        	if (state == null || state != targetState) {
        		System.out.printf("Incomplete:\t%s\t%s\n", im, state);
        	}
        }
        return qs;
	}

	private void randomAwait(int minMillis, int maxMillis) {
    	lock.lock();
    	try {
    		ThreadUtil.randomAwait(cv, minMillis, maxMillis);
    	} finally {
    		lock.unlock();
    	}
    }
    
    private void signalAwait() {
    	lock.lock();
    	try {
    		cv.signalAll();
    	} finally {
    		lock.unlock();
    	}
    }
    
    class StateChecker extends TimerTask {
        StateChecker() {
        }
        
        @Override
        public void run() {
            ensureStateSet();
        }
    }
}

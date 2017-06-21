package com.ms.silverking.cloud.dht.daemon;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.SuspectsZK;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.SuspectAddressListener;
import com.ms.silverking.net.async.SuspectProblem;

public class PeerHealthMonitor implements SuspectAddressListener {
    private Set<IPAndPort>  currentSuspects;
    private Set<IPAndPort>  currentWeakSuspects;
    private Set<IPAndPort>  missingInZKSuspects;
    private Set<IPAndPort>  communicationSuspects;
    private SuspectsZK      suspectsZK;
    private IPAndPort       localIPAndPort;
    private ConcurrentMap<IPAndPort,Long>	lastWeakErrorTimes;
    
    /*
     * Weak suspects are still members of the system. They are simply de-prioritized for reads.
     * Any truly bad member needs to be added as a suspect proper, which may trigger a topology change.
     */
    
    // FUTURE - make configurable, or update algorithm
    private static final long	weakErrorTimeoutMillis = 5 * 60 * 1000;
    
    private static final boolean    verbose = true;
    
    public PeerHealthMonitor(MetaClient mc, IPAndPort localIPAndPort) throws KeeperException {
        currentSuspects = new ConcurrentSkipListSet<>();
        currentWeakSuspects = new ConcurrentSkipListSet<>();
        missingInZKSuspects = new ConcurrentSkipListSet<>();
        communicationSuspects = new ConcurrentSkipListSet<>();
    	lastWeakErrorTimes = new ConcurrentHashMap<>();
        if (mc != null) {
        	suspectsZK = new SuspectsZK(mc);
        } else {
        	Log.warning("PeerHealthMonitor in unit test mode");
        	suspectsZK = null;
        }
        this.localIPAndPort = localIPAndPort;
    }
    
    public void initialize() {
        updateZK();
    }

    public boolean isSuspect(IPAndPort peer) {
    	if (currentSuspects.contains(peer)) {
    		return true;
    	} else {
    		if (currentWeakSuspects.contains(peer)) {
    			Long	lastWeakErrorTime;
    			
    			lastWeakErrorTime = lastWeakErrorTimes.get(peer);
    			if (lastWeakErrorTime != null && SystemTimeUtil.systemTimeSource.absTimeMillis() - lastWeakErrorTime <= weakErrorTimeoutMillis) {
    				return true;
    			} else {
    				// possibly take action
    				return false;
    			}
    		} else {
    			return false;
    		}
    	}
    }
    
    public long getLastWeakErrorTime(IPAndPort peer) {
    	Long	t;
    	
    	t = lastWeakErrorTimes.get(peer);
    	if (t == null) {
    		return Long.MAX_VALUE;
    	} else {
    		if (SystemTimeUtil.systemTimeSource.absTimeMillis() - t <= weakErrorTimeoutMillis) { 
    			return t;
    		} else {
        		return Long.MAX_VALUE;
    		}
    	}
    }

    
    
    @Override
    public void addSuspect(InetSocketAddress peer, Object rawCause) {
    	PeerHealthIssue	issue;
    	
    	if (rawCause instanceof PeerHealthIssue) {
    		issue = (PeerHealthIssue)rawCause;
    	} else if (rawCause instanceof SuspectProblem) {
    		switch ((SuspectProblem)rawCause) {
    		case ConnectionEstablishmentFailed:
    			issue = PeerHealthIssue.CommunicationError;
    			break;
    		case CommunicationError:
    			issue = PeerHealthIssue.CommunicationError;
    			break;
    		default:
    			throw new RuntimeException("Panic");
    		}
    	} else {
    		throw new RuntimeException("Panic");
    	}
    	addSuspect(new IPAndPort(peer), issue);
    }
    	
    public void addSuspect(IPAndPort peer, PeerHealthIssue issue) {
        Log.warning("addSuspect: "+ peer +" "+ issue);
        switch (issue) {
        case ReplicaTimeout:
        	// For now, only treat replica timeouts as a weak suspect hint
        	addWeakSuspect(peer);
            //communicationSuspects.add(peer);
            //checkForStrongSuspect(peer);
            break;
        case MissingInZooKeeperAfterTimeout:
        	// fall through
        case MissingInZooKeeper:
        	addWeakSuspect(peer);
            missingInZKSuspects.add(peer);
            checkForStrongSuspect(peer);
        	break;
        case CommunicationError:
        	// fall through
        default:
        	addStrongSuspect(peer, issue.toString());
	        break;
        }
    }
    
    private void addWeakSuspect(IPAndPort peer) {
        currentWeakSuspects.add(peer);
    	lastWeakErrorTimes.put(peer, SystemTimeUtil.systemTimeSource.absTimeMillis());
    }

    private void addStrongSuspect(IPAndPort peer, String cause) {
        if (verbose) {
            Log.warningAsync("PeerHealthMonitor.addSuspect (strong): "+ peer +" "+ cause);
        }
        if (currentSuspects.add(peer)) {
        	updateZK();
        }
	}

	private void checkForStrongSuspect(IPAndPort peer) {
    	if (missingInZKSuspects.contains(peer) && communicationSuspects.contains(peer)) {
    	//if (communicationSuspects.contains(peer)) {
    		addStrongSuspect(peer, "checkForStrongSuspect");
    	}
	}

	@Override
    public void removeSuspect(InetSocketAddress addr) {
        removeSuspect(new IPAndPort(addr));
    }
    
    public void removeSuspect(IPAndPort peer) {
    	boolean	removed;
    	
        if (verbose) {
            Log.fineAsync("PeerHealthMonitor.removeSuspect ", peer);
        }
        missingInZKSuspects.remove(peer);
        communicationSuspects.remove(peer);
        currentWeakSuspects.remove(peer);
        if (currentSuspects.remove(peer)) {
        	updateZK();
        	removed = true;
        } else {
        	removed = false;
        }
        if (removed && verbose) {
            Log.fineAsync("PeerHealthMonitor.removeSuspect, removed: "+ peer);
        }
    }
    
    /**
     * Ensure that ZK is up to date with local state. Read and verify before writing
     * @throws KeeperException
     */
    public void refreshZK() throws KeeperException {
    	Set<IPAndPort>	zkSuspects;
    	
    	zkSuspects = suspectsZK.readSuspectsFromZK(localIPAndPort);
    	if (zkSuspects == null || !zkSuspects.equals(currentSuspects)) {
    		updateZK();
    	}
    }
    
    private void updateZK() {
        try {
        	if (suspectsZK != null) {
        		suspectsZK.writeSuspectsToZK(localIPAndPort, currentSuspects);
        		Log.warning("Current Suspects: ", CollectionUtil.toString(currentSuspects));
        	}
        } catch (Exception e) {
            Log.logErrorWarning(e);
        }
    }
}

package com.ms.silverking.cloud.dht.daemon;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.dht.common.DHTUtil;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingState;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.RingStateZK;
import com.ms.silverking.cloud.meta.ChildrenListener;
import com.ms.silverking.cloud.meta.ChildrenWatcher;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class PeerStateWatcher implements ChildrenListener {
    private final DHTConfiguration  dhtConfig;
    private final Pair<Long,Long>	ringVersionPair;
    private final Set<IPAndPort>    peers;
    private final RingStateZK       ringStateZK;
    private final ChildrenWatcher   childrenWatcher;
    private volatile RingState      targetState;
    private final PeerStateListener peerStateListener;
    private final Stopwatch			peerInitialZKUpdateTimeoutSW;
    private final Set<IPAndPort>	nodesMissingInZK;
    private final Set<RingState>	statesMet;
    private volatile boolean	active;
    private final TimeoutTask	timeoutTask;
    
    private static final int	peerInitialZKUpdateTimeoutMillis = 1 * 60 * 1000;

    private static PeerHealthMonitor	peerHealthMonitor;
    
    public static void setPeerHealthMonitor(PeerHealthMonitor _peerHealthMonitor) {
    	peerHealthMonitor = _peerHealthMonitor; 
    }
    
    public PeerStateWatcher(Set<IPAndPort> peers, 
                     com.ms.silverking.cloud.dht.meta.MetaClient dhtMC, DHTConfiguration dhtConfig, 
                     Pair<Long,Long> ringVersionPair,
                     PeerStateListener peerStateListener,
                     RingState targetState) throws KeeperException {
    	active = true;
        this.peers = peers;
        ringStateZK = new RingStateZK(dhtMC, dhtConfig, ringVersionPair); 
        childrenWatcher = new ChildrenWatcher(RingMapState.peerStateWatcherTimer, dhtMC, ringStateZK.getRingInstanceStatePath(), 
                this, RingMapState.peerStateCheckIntervalMillis, RingMapState.peerStateInitialIntervalMillis);
        this.dhtConfig = dhtConfig;
        this.ringVersionPair = ringVersionPair;
        this.peerStateListener = peerStateListener;
        this.targetState = targetState;
        peerInitialZKUpdateTimeoutSW = new SimpleStopwatch();
        nodesMissingInZK = new ConcurrentSkipListSet<>();
        statesMet = new ConcurrentSkipListSet<>();
        
        timeoutTask = new TimeoutTask();
		DHTUtil.timer().schedule(timeoutTask, peerInitialZKUpdateTimeoutMillis);
    	Log.warning("Created PeerStateWatcher "+ ringVersionPair);        
    }
    
    public PeerStateWatcher(Set<IPAndPort> peers, 
            com.ms.silverking.cloud.dht.meta.MetaClient dhtMC, DHTConfiguration dhtConfig, 
            Pair<Long,Long> ringVersionPair,
            PeerStateListener peerStateListener) throws KeeperException {
    	this(peers, dhtMC, dhtConfig, ringVersionPair, peerStateListener, null);
    }
    
    public void stop() {
    	active = false;
    	Log.warning("Stopping PeerStateWatcher "+ ringVersionPair);
    	timeoutTask.cancel();
        childrenWatcher.stop();
    }

    void setTargetState(RingState targetState) {
        this.targetState = targetState;
    }

    @Override
    public void childrenChanged(String basePath, Map<String, byte[]> childStates) {
    	if (active) {
	        RingState   _targetState;
	        
	        if (RingMapState.verbosePeerStateCheck) {
	            Log.warning("PeerStateWatcher.childrenChanged: ", basePath);
	        }
	        _targetState = targetState;
	        if (_targetState != null && stateMet(_targetState, childStates)) {
	            if (RingMapState.verbosePeerStateCheck) {
	                Log.warning("PeerStateWatcher calling peerStateMet: ", basePath);
	            }
	            if (statesMet.add(targetState)) {
	            	peerStateListener.peerStateMet(dhtConfig, ringVersionPair, _targetState);
	            }
	        }
    	}
    }
    
	class TimeoutTask extends TimerTask {
		public void run() {
			if (active) {
				for (IPAndPort node : nodesMissingInZK) {
					Log.warning("Timed out after missing in ZK: ", node);
					try {
						peerHealthMonitor.addSuspect(node, PeerHealthIssue.MissingInZooKeeperAfterTimeout);
					} catch (Exception e) {
						Log.logErrorWarning(e);
					}
				}
			}
		}
	}
    
    private void considerForTimeout(IPAndPort node) {
    	if (active) {
			boolean	doTimeout;
			
			doTimeout = peerInitialZKUpdateTimeoutSW.getSplitMillis() > peerInitialZKUpdateTimeoutMillis;
			if (peerHealthMonitor != null && doTimeout) {
				Log.warning("Missing in ZK and timed out: ", node);
				peerHealthMonitor.addSuspect(node, PeerHealthIssue.MissingInZooKeeperAfterTimeout);
			} else {
				nodesMissingInZK.add(node);
			}
    	}
    }
    
    private boolean stateMet(RingState targetState, Map<String, byte[]> childStates) {
    	if (active) {
	    	nodesMissingInZK.clear();
	        if (RingMapState.debug) {
	            Log.warning("in PeerStateWatcher.stateMet ", targetState);
	        }
	        if (childStates.size() >= peers.size()) {
	            Set<IPAndPort>  nodesToCheck;
	            
	            if (RingMapState.debug) {
	                Log.warning("childStates.size() >= peers.size()");
	            }
	            if (targetState.requiresPassiveParticipation()) {
	                // FUTURE - this won't have any effect until passive nodes are adding themselves
	                nodesToCheck = new HashSet<>(peers);
	                for (String nodeDef : childStates.keySet()) {
	                    nodesToCheck.add(new IPAndPort(nodeDef));
	                }
	            } else {
	                nodesToCheck = peers;
	            }
	            for (IPAndPort node : nodesToCheck) {
	                byte[]      nodeStateRaw;
	                RingState   nodeState;
	                
	                nodeStateRaw = childStates.get(node.toString());
	                nodeState = RingState.valueOf(nodeStateRaw);
	                if (!targetState.metBy(nodeState)) {
	                    //if (debug) {
	                        Log.warning(String.format("targetState not found for: %s nodeState %s targetState %s %s", 
	                                node.toString(), nodeState, targetState, targetState.metBy(nodeState)));
	                        if (nodeState == null) {
	                        	considerForTimeout(node);
	                        }
	                    //}
	                    return false;
	                }
	            }
	            if (RingMapState.debug) {
	                Log.warning("stateMet");
	            }
	            return true;
	        } else {
	            if (RingMapState.debug) {
	                Log.warning("childStates.size() < peers.size()");
	                for (IPAndPort peer : peers) {
	                	if (!childStates.containsKey(peer.toString())) {
	                		Log.warning("Couldn't find state for: "+ peer);
	                    	considerForTimeout(peer);
	                	} else {
	                		//peerHealthMonitor.removeSuspect(peer.toString());
	                	}
	                }
	            }
	            return false;
	        }
    	} else {
    		return false;
    	}
    }
}
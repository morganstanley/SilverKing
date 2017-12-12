package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.storagepolicy.ReplicationType;
import com.ms.silverking.net.IPAndPort;

class RetrievalEntrySingleState extends BaseRetrievalEntryState {
    private final List<IPAndPort>   primaryReplicas;
    private final List<IPAndPort>   secondaryReplicas;
    private RetrievalState  state;
    private short           replicaIndex;
    private short           prevReplicaIndex;
        // 0...(secondaryReplicas.size() - 1) ==> secondary
        // secondaryReplicas.size()...((secondaryReplicas.size() + (primaryReplicas.size() - 1))) ==> primary
    
    private static final boolean    verifyReplicas = false;
    
    RetrievalEntrySingleState(List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas) {
        if (verifyReplicas) {
        	Preconditions.checkArgument( !primaryReplicas.contains(null) );
        	Preconditions.checkArgument( !secondaryReplicas.contains(null) );
        }
        this.primaryReplicas = primaryReplicas;
        this.secondaryReplicas = secondaryReplicas;
        state = RetrievalState.INITIAL;
        replicaIndex = -1;
        prevReplicaIndex = -1;
    }
    
    public List<IPAndPort> getSecondaryReplicas() {
        return secondaryReplicas;
    }
    
    public ReplicationType replicaType(IPAndPort ipAndPort) {
        if (primaryReplicas.contains(ipAndPort)) {
            return ReplicationType.Primary;
        } else if (secondaryReplicas.contains(ipAndPort)) {
            return ReplicationType.Secondary;
        } else {
            return null;
        }
    }
    
    public boolean isPrimaryReplica(IPAndPort replica) {
        return primaryReplicas.contains(replica);
    }
    
    private boolean currentReplicaIsSecondary() {
        return replicaIndex < secondaryReplicas.size();
    }
    
    @Override
    public IPAndPort getInitialReplica() {
        replicaIndex = 0;
        if (secondaryReplicas.size() > 0) {
            return secondaryReplicas.get(0);
        } else {
            return primaryReplicas.get(0);
        }
    }
    
    private IPAndPort getReplica(int index) {
    	if (index < 0) {
    		return null;
    	} else {
	    	if (index < secondaryReplicas.size()) {
	    		return secondaryReplicas.get(index);
	    	} else {
	            return primaryReplicas.get(index - secondaryReplicas.size());
	    	}
        }
    }

	@Override
	public boolean prevReplicaSameAsCurrent() {
		return prevReplicaIndex == replicaIndex;
	}
    
    @Override
    public IPAndPort currentReplica() {
    	return getReplica(Math.max(replicaIndex, 0));
    }
    
    @Override
    public IPAndPort nextReplica() {
        // Mutual exclusion is guaranteed externally (synchronized block in SimpleRetrievalOperation)
        
        // Current logic is single try on a secondary replica and then move through all replicas        
        // (FUTURE - consider moving the replica logic out of here)
        
        // Replicas with index 0...secondaryReplicas.size() - 1 are secondary replicas
        // Replicas with index 
        // secondaryReplicas.size()...primaryReplicas.size() + secondaryReplicas.size() - 1 are primary replicas
        
    	prevReplicaIndex = replicaIndex;
        if (replicaIndex < 0) {
            incrementReplicaTimeout();
            replicaIndex = 0;
            if (secondaryReplicas.size() > 0) {
                // Try one secondary replica if any exist
                return secondaryReplicas.get(0);
            } else {
                // No secondary replicas, just go to primary
                return primaryReplicas.get(0);
            }
        } else {
            if (currentReplicaIsSecondary()) {
                // Secondaries have been tried. Now move on to first primary replica
                replicaIndex = (short)secondaryReplicas.size();
            } else {
                if (replicaIndex >= secondaryReplicas.size() + primaryReplicas.size() - 1) {
                    // All replicas tried already. Return null
                    return null;
                } else {
                    replicaIndex++;
                }
            }
            incrementReplicaTimeout();
            return primaryReplicas.get(replicaIndex - secondaryReplicas.size());
        }
    }
    
    void setState(RetrievalState state) {
        if (!this.state.validTransition(state)) {
            throw new RuntimeException("Invalid transition: "+ this.state +" -> "+ state);
        } else {
            this.state = state;
        }
    }
    
    RetrievalState getState() {
        return state;
    }
    
	@Override
	public boolean isComplete() {
		return state.isComplete();
	}
	
    @Override
    public String toString() {
        return primaryReplicas.toString() +":"+ secondaryReplicas.toString() +":"+ state.toString();
    }
}

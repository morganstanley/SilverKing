package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;


/**
 * Not thread-safe. Only to be used for a single processing pass.
 */
public class RetrievalCommunicator extends OpCommunicator<DHTKey,RetrievalResult> 
                                   implements RetrievalVirtualCommunicator {
    private List<SecondaryReplicasUpdate>    secondaryReplicasUpdates;
    
    private static final int    initialSecondaryReplicaUpdateListSize = 4;
    
    private static final boolean    debug = false;
    
    public RetrievalCommunicator() {
    }
    
    ////////////////
    
    @Override
    public void sendResult(RetrievalResult result, List<IPAndPort> secondaryReplicas) {
        if (debug) {
            Log.warning("RetrievalCommunicator.sendResult() ", secondaryReplicas.size());
        }
        if (secondaryReplicas.size() > 0) {
            synchronized (this) {
                if (secondaryReplicasUpdates == null) {
                    secondaryReplicasUpdates = new ArrayList<>(initialSecondaryReplicaUpdateListSize);
                }
                secondaryReplicasUpdates.add(new SecondaryReplicasUpdate(secondaryReplicas, result)); 
            }
        }
        super.sendResult(result);
    }
    
    public List<SecondaryReplicasUpdate> getSecondaryReplicasUpdates() {
        return secondaryReplicasUpdates;
    }
}

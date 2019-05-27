package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

class StorageEntrySingleState extends StorageEntryState {
    private List<IPAndPort> replicas;
    private OpResult[]      replicaResults;

    StorageEntrySingleState(List<IPAndPort> replicas) {
        super();
        this.replicas = replicas;
        replicaResults = new OpResult[replicas.size()];
        for (int i = 0; i < replicas.size(); i++) {
            replicaResults[i] = OpResult.INCOMPLETE;
        }
    }

    @Override
    OpResult getCurOpResult() {
        OpResult    result;
        
        result = OpResult.INCOMPLETE;
        for (int i = 0; i < replicas.size(); i++) {
            OpResult    replicaResult;
            
            replicaResult = replicaResults[i];
            if (replicaResult.isComplete()) {
                if (replicaResult.hasFailed()) {
                    if (result == OpResult.INCOMPLETE) {
                        result = replicaResult;
                    } else {
                        if (result != replicaResult) {
                            result = OpResult.MULTIPLE;
                        }
                    }
                } else {
                    result = OpResult.SUCCEEDED;
                }
            }
        }
        return result;
    }
    
    void setReplicaResult(IPAndPort replica, OpResult result) {
        int index;
        
        index = replicas.indexOf(replica);
        if (!replicaResults[index].isComplete()) {
            replicaResults[index] = result;
        } else {
            Log.warning("Attempted update of complete: ", replica +" "+ replicaResults[index] +" "+ result);
        }
    }
}

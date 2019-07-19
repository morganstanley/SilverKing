package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyedResult;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.net.IPAndPort;


/**
 * Superinterface of all storage and retrieval operations. 
 */
public interface Operation<T extends DHTKey,R extends KeyedResult> {
    public void processInitialMessageGroupEntry(T entry, 
            List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas, OpVirtualCommunicator<T,R> vComm);
    /**
     * Result of operation
     * @return operation result
     */
    public OpResult getOpResult();
}

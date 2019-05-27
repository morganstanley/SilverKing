package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.Set;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.net.IPAndPort;

/**
 * An operation that retrieves values from the DHT 
 */
public interface RetrievalOperation extends Operation<DHTKey,RetrievalResult> {
    /**
     * Update a key/replica for this operation 
     * @param key
     * @param replica
     * @param update
     */
    public void update(DHTKey key, IPAndPort replica, RetrievalResult update, RetrievalVirtualCommunicator rvComm);
    public Set<IPAndPort> checkForInternalTimeouts(long curTimeMillis, RetrievalVirtualCommunicator rComm);
}

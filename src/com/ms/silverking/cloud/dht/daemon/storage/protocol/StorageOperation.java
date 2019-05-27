package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyEntry;
import com.ms.silverking.cloud.dht.net.PutResult;
import com.ms.silverking.net.IPAndPort;

/**
 * An operation that stores values in the DHT. 
 */
public interface StorageOperation extends Operation<MessageGroupKeyEntry,PutResult> {
    /**
     * Update a key/replica for this operation 
     * @param key
     * @param replica
     * @param storageState TODO
     * @param update
     * @param pvComm TODO
     */
    public void update(DHTKey key, IPAndPort replica, byte storageState, OpResult update, PutVirtualCommunicator pvComm);
    public byte nextStorageState(byte storageState);
    public byte initialStorageState();
    public void localUpdate(DHTKey key, byte storageState, OpResult update, PutVirtualCommunicator pvComm);
}

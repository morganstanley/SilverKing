package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.net.IPAndPort;

/**
 * Provides external non-protocol specific functionality that is required by Operations.
 */
public interface OperationContainer {
    public IPAndPort localIPAndPort();
    public boolean isLocalReplica(IPAndPort replica);
    public boolean containsLocalReplica(List<IPAndPort> replicas);
    public StorageModule getStorage();
    public long getContext();
    public byte[] getValueCreator();
    public int getNumEntries();
    public OpResult getOpResult();
}

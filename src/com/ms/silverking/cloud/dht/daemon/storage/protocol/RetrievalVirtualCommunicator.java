package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.net.IPAndPort;


public interface RetrievalVirtualCommunicator extends OpVirtualCommunicator<DHTKey,RetrievalResult> {
//    public void sendOpUpdate(DHTKey key, IPAndPort replica, OpUpdate opUpdate);
//    public void sendOpResult(DHTKey key, OpResult opResult);
    public void sendResult(RetrievalResult result, List<IPAndPort> secondaryReplicas);
}

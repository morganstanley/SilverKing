package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.daemon.storage.KeyedOpResultListener;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyOrdinalEntry;
import com.ms.silverking.cloud.dht.net.PutResult;
import com.ms.silverking.net.IPAndPort;

public interface PutVirtualCommunicator extends OpVirtualCommunicator<MessageGroupKeyEntry,PutResult>, KeyedOpResultListener {
    public void forwardUpdateEntry(IPAndPort replica, MessageGroupKeyOrdinalEntry entry);
    public boolean isLocalReplica(IPAndPort replica);
}

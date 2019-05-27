package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyEntry;
import com.ms.silverking.net.IPAndPort;

public interface OperationResultHandler<K extends MessageGroupKeyEntry> {
    public void sendToOriginator(MessageGroup messageGroup);
    public void sendToReplica(IPAndPort replica, K entry);
}

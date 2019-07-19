package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyedResult;
import com.ms.silverking.net.IPAndPort;

/**
 * Used during processing of either an incoming forward message or an
 * incoming response. All entries of the message must be processed
 * and then either forwarded, or resolved locally. In the local 
 * resolution case, a result must be sent back.
 * 
 * For efficiency purposes, we need to group both the forwards and
 * results. This class groups messages for forwarding by destination. 
 * It also groups results for sending in batches.
 * 
 * Client classes are provided with the illusion of single object
 * sends in both cases while we maintain efficiency in the actual
 * implementation.
 */
public interface OpVirtualCommunicator<T extends DHTKey,R extends KeyedResult> {
    public void forwardEntry(IPAndPort replica, T entry);
    public void sendResult(R result);
}

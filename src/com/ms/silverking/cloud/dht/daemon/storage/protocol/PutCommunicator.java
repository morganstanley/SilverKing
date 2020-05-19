package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyOrdinalEntry;
import com.ms.silverking.cloud.dht.net.PutResult;
import com.ms.silverking.net.IPAndPort;

/**
 * Not thread-safe. Only to be used for a single processing pass.
 */
public class PutCommunicator extends OpCommunicator<MessageGroupKeyEntry, PutResult> implements PutVirtualCommunicator {
  private Map<IPAndPort, List<MessageGroupKeyOrdinalEntry>> replicaUpdateMessageLists;
  private final PutOperationContainer putOperationContainer;

  private static final boolean debug = false;

  public PutCommunicator(PutOperationContainer putOperationContainer) {
    this.putOperationContainer = putOperationContainer;
    replicaUpdateMessageLists = new HashMap<>(typicalReplication);
  }

  public PutOperationContainer getPutOperationContainer() {
    return putOperationContainer;
  }

  public void sendResult(DHTKey key, OpResult result) {
    super.sendResult(new PutResult(key, result));
  }

  public void forwardUpdateEntry(IPAndPort replica, MessageGroupKeyOrdinalEntry entry) {
    List<MessageGroupKeyOrdinalEntry> messageList;

    assert replica != null;
    messageList = replicaUpdateMessageLists.get(replica);
    if (messageList == null) {
      messageList = new ArrayList<>(initialListSize);
      replicaUpdateMessageLists.put(replica, messageList);
    }
    messageList.add(entry);
  }

  public Map<IPAndPort, List<MessageGroupKeyOrdinalEntry>> getReplicaUpdateMessageLists() {
    return replicaUpdateMessageLists;
  }

  public Map<IPAndPort, List<MessageGroupKeyOrdinalEntry>> takeReplicaUpdateMessageLists() {
    Map<IPAndPort, List<MessageGroupKeyOrdinalEntry>> oldReplicaUpdateMessageLists;
    oldReplicaUpdateMessageLists = replicaUpdateMessageLists;
    replicaUpdateMessageLists = new HashMap<>(typicalReplication);
    return oldReplicaUpdateMessageLists;
  }

  @Override
  public boolean isLocalReplica(IPAndPort replica) {
    return putOperationContainer.isLocalReplica(replica);
  }
}

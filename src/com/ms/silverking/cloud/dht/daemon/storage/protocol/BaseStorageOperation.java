package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupPutEntry;
import com.ms.silverking.cloud.dht.net.PutResult;
import com.ms.silverking.net.IPAndPort;

/**
 * Common StorageOperation functionality.
 */
public abstract class BaseStorageOperation<S> extends BaseOperation<S> implements StorageOperation {
    private final PutOperationContainer putOperationContainer;
    
    private static final boolean    debug = false;
    
    public BaseStorageOperation(long deadline, PutOperationContainer putOperationContainer, 
                                ForwardingMode forwardingMode) {
        super(deadline, putOperationContainer, forwardingMode, StorageEntryState.minRelTimeoutMillis, 
              putOperationContainer.getNumEntries());
        this.putOperationContainer = putOperationContainer;
    }

    @Override
    public void processInitialMessageGroupEntry(MessageGroupKeyEntry _entry, 
                                        List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas,
                                        OpVirtualCommunicator<MessageGroupKeyEntry, PutResult> pvComm) {
        MessageGroupPutEntry entry;
        
        if (debug) {
            System.out.printf("processInitialMessageGroupEntry() %d\n", primaryReplicas.size());
        }
        entry = (MessageGroupPutEntry)_entry;
        if (forwardingMode.forwards()) {
            if (putOperationContainer.getSecondaryTargets() == null) {
                // Eagerly write to secondary replicas *only* if targets have
                // been defined. FUTURE - could change.
                secondaryReplicas = ImmutableList.of();
            }
            initializeEntryState(entry, primaryReplicas, secondaryReplicas);
            for (IPAndPort replica : primaryReplicas ) {
                pvComm.forwardEntry(replica, entry);
            }
            if (putOperationContainer.getSecondaryTargets() != null) {
                for (IPAndPort replica : secondaryReplicas ) {
                    pvComm.forwardEntry(replica, entry);
                }
            }
        } else {
            pvComm.forwardEntry(putOperationContainer.localIPAndPort(), entry);
        }
    }
    
    @Override
    public void localUpdate(DHTKey key, byte storageState, OpResult update, PutVirtualCommunicator pvComm) {
        update(key, putOperationContainer.localIPAndPort(), storageState, update, pvComm);        
    }

    public abstract void initializeEntryState(DHTKey entryKey, 
            List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas);
    
    public byte initialStorageState() {
        // only necessary to override if 0 is not the ordinal value of a given
        // prototol's initial storage state
        return (byte)0;
    }
    
    public byte nextStorageState(byte prevStorageState) {
        // this method should be overridden by protocols that expect this call
        throw new RuntimeException("Unexpected nextStorageState() call");
    }
}

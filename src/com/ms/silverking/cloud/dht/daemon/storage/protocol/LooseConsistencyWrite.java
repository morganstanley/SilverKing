package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

/**
 * Write operation for the LooseConsistency StorageProtocol.
 */
public class LooseConsistencyWrite extends BaseStorageOperation<StorageEntrySingleState> {
    private static final boolean    debug = false;
    
    LooseConsistencyWrite(PutOperationContainer putOperationContainer, ForwardingMode forwardingMode, long deadline) {
        super(deadline, putOperationContainer, forwardingMode);
    }
    
    @Override
    public void initializeEntryState(DHTKey entryKey, List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas) {
        setEntryState(entryKey, new StorageEntrySingleState(primaryReplicas));
    }


    @Override
    public void update(DHTKey key, IPAndPort replica, byte storageState, OpResult update, PutVirtualCommunicator pvComm) {
        // FIXME - reduce or eliminate locking here
        synchronized (this) {
            StorageEntrySingleState entryState;

            if (debug) {
                System.out.printf("replica %s\tupdate %s\n", replica, update);
            }
            entryState = getEntryState(key);
            if (debug) {
                System.out.printf("curOpResult %s\n", entryState.getCurOpResult());
            }
            if (entryState.getCurOpResult() == OpResult.INCOMPLETE) {
                entryState.setReplicaResult(replica, update);
                if (update.isComplete()) {
                    OpResult    looseResult;
                    
                    looseResult = entryState.getCurOpResult();
                    if (debug) {
                        System.out.printf("looseResult %s\n", looseResult);
                    }
                    if (looseResult.isComplete()) {
                        int _completeEntries;
                        
                        pvComm.sendResult(key, looseResult);
                        _completeEntries = completeEntries.incrementAndGet();
                        if (_completeEntries >= numEntries) {
                            setOpResult(OpResult.SUCCEEDED);
                        }                    
                    }
                    if (debug) {
                        System.out.printf("completeEntries %d numEntries %d\n", completeEntries.get(), numEntries);
                    }
                } else {
                    Log.warning("Unexpected incomplete update: ", update);
                }
            } else {
                if (Log.levelMet(Level.FINE)) {
                    Log.fine("Update for non-incomplete: ", key + " " + replica + " " + update);
                }
            }
        }
    }
    
    public byte nextStorageState(byte prevStorageState) {
        return 0;
    }
}

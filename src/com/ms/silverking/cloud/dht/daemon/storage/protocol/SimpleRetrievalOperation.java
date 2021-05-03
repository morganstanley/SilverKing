package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

/**
 * A retrieval operation that expects all primary replicas to be authoritative.
 * Secondary replicas are not authoritative.
 */
class SimpleRetrievalOperation extends BaseRetrievalOperation<RetrievalEntrySingleState> {
  SimpleRetrievalOperation(long deadline, RetrievalOperationContainer retrievalOperationContainer,
      ForwardingMode forwardingMode) {
    super(deadline, retrievalOperationContainer, forwardingMode);
  }

  @Override
  protected RetrievalEntrySingleState initializeEntryState(DHTKey entryKey, List<IPAndPort> primaryReplicas,
      List<IPAndPort> secondaryReplicas) {
    RetrievalEntrySingleState entryState;

    if (debug) {
      System.out.printf("initializeEntryState %s %s %s\n", entryKey, CollectionUtil.toString(primaryReplicas),
          CollectionUtil.toString(secondaryReplicas));
    }
    entryState = new RetrievalEntrySingleState(primaryReplicas, secondaryReplicas);
    setEntryState(entryKey, entryState);
    return entryState;
  }
  
  protected void noPrimaryReplicasForKey(DHTKey key, List<IPAndPort> primaryReplicas,
      List<IPAndPort> secondaryReplicas, OpVirtualCommunicator<DHTKey, RetrievalResult> rvComm) {
    RetrievalEntrySingleState entryState;
    
    entryState = initializeEntryState(key, primaryReplicas, secondaryReplicas);
    entryState.updatePresentResult(OpResult.REPLICA_EXCLUDED);
    entryState.setComplete();
    synchronized (rvComm) {
      rvComm.sendResult(new RetrievalResult(key, OpResult.REPLICA_EXCLUDED, null));
    }
  }
  
  @Override
  public void update(DHTKey key, IPAndPort replica, RetrievalResult update, RetrievalVirtualCommunicator rvComm) {
    RetrievalEntrySingleState entryState;

    if (debug) {
      Log.warning("SimpleRetrievalOperation.update(): ", key + " " + replica + " " + update);
    }

    // Sanity check the update
    if (replica == null) {
      Log.info("Ignoring update for null replica");
      return;
    }
    entryState = getEntryState(key);
    if (entryState == null) {
      Log.infof("null entry state %s %s %s\n", key, replica, update);
      return;
    }
    if (!entryState.isReplica(replica)) {
      Log.infof("Ignoring update for other replica %s", replica);
      return;
    }

    // Update is sane, process
    synchronized (entryState) {
      // FUTURE - need to handle potentially incomplete success
      // (generated when we get a value that we're not sure about whether a newer value is around)
      // leave out for now
      
      // process this update if it is a success from any replica, or if it is a failure from the current replica
      if (!update.getResult().hasFailed() || entryState.currentReplica().equals(replica)) {
        if (forwardingMode == ForwardingMode.DO_NOT_FORWARD) {
          // Update is from this replica; forward the result
          synchronized (rvComm) {
            rvComm.sendResult(update);
          }
          entryState.setComplete();
        } else {
          boolean complete;
          
          // forwardingMode is either FORWARD or ALL
          // Update is from a remote replica
          switch (update.getResult()) {
          case CORRUPT: // fall through
          case REPLICA_EXCLUDED:
            entryState.updatePresentResult(update.getResult());
            complete = false;
            break;
          case NO_SUCH_VALUE:
            entryState.updatePresentResult(update.getResult());
            if (entryState.isPrimaryReplica(replica) && forwardingMode != ForwardingMode.ALL) {
              synchronized (rvComm) {
                rvComm.sendResult(update);
              }
              entryState.setComplete();
              complete = true;
            } else {            
              complete = false;
            }
            break;
          case SUCCEEDED:
            entryState.updatePresentResult(update.getResult());
            entryState.setComplete();
            if (entryState.isPrimaryReplica(replica)) {
              synchronized (rvComm) {
                rvComm.sendResult(update, entryState.getSecondaryReplicas());
              }
            } else {
              synchronized (rvComm) {
                rvComm.sendResult(update);
              }
            }
            complete = true;
            break;
          default:
            throw new RuntimeException("Unexpected update result: " + update.getResult());
          }       
          
          if (complete) {
            int _completeEntries;
            
            // remove from map when complete?
            _completeEntries = completeEntries.incrementAndGet();
            if (_completeEntries >= numEntries) {
              setOpResult(OpResult.SUCCEEDED); // triggers removal from MessageModule map
            }
          } else {
            // Move to next replica if any remain
            IPAndPort nextReplica;
            
            // The current replica has failed; move to the next replica
            nextReplica = entryState.nextReplica();
            if (debug) {
              System.out.printf("forward entry state %s %s\n", key, nextReplica);
            }
            
            if (nextReplica == null) {
              // All replicas exhausted; send the best result that we have
              // but leave operation as incomplete unless the following condition is met
              if (forwardingMode == ForwardingMode.ALL && entryState.getPresentResult() == OpResult.NO_SUCH_VALUE) {
                entryState.setComplete();
              }
              synchronized (rvComm) {
                rvComm.sendResult(new RetrievalResult(key, entryState.getPresentResult(), null));
              }
            } else {
              synchronized (rvComm) {
                rvComm.forwardEntry(nextReplica, key);
              }
            }
          }
        }
      } else {
        if (Log.levelMet(Level.INFO)) {
          Log.infof("Ignoring update: %s %s %s", update.getResult().hasFailed(), entryState.currentReplica(), replica);
        }
      }
    }
  }
  
  public void replicaIncluded(IPAndPort replica, RetrievalCommunicator rComm) {
    if (forwardingMode != ForwardingMode.DO_NOT_FORWARD) {
      for (DHTKey key : opKeys()) {
        replicaIncluded(key, replica, rComm);
      }
    }
  }

  private void replicaIncluded(DHTKey key, IPAndPort replica, RetrievalCommunicator rComm) {
    RetrievalEntrySingleState entryState;

    if (debug) {
      Log.warning("SimpleRetrievalOperation.replicaIncluded(): ", key + " " + replica);
    }
    
    if (forwardingMode == ForwardingMode.DO_NOT_FORWARD) {
      throw new RuntimeException("Invalid replicaIncluded() invocation");
    }    

    entryState = getEntryState(key);
    if (entryState == null) {
      if (debug) {
        System.out.printf("null entry state %s %s\n", key, replica);
      }
      return;
    }

    synchronized (entryState) {
      if (entryState.isComplete() && entryState.getPresentResult() != OpResult.REPLICA_EXCLUDED) {
        return;
      } else {
        IPAndPort currentReplica;
        
        currentReplica = entryState.currentReplica();
        if (currentReplica != null && currentReplica.equals(replica)) {      
          if (Log.levelMet(Level.INFO)) {
            Log.infof("forwarding: %s => %s", KeyUtil.keyToString(key), replica);
          }
          synchronized (rComm) {
            rComm.forwardEntry(replica, key);
          }
        }
      }
    }
  }
  
  public void replicaExcluded(IPAndPort replica, RetrievalCommunicator rComm) {
    if (Log.levelMet(Level.INFO)) {
      Log.infof("SimpleRetrievalOperation.replicaExcluded %s", replica);
    }
    if (forwardingMode != ForwardingMode.DO_NOT_FORWARD) {
      for (DHTKey key : opKeys()) {
        replicaExcluded(key, replica, rComm);
      }
    }
  }
  
  private void replicaExcluded(DHTKey key, IPAndPort replica, RetrievalCommunicator rComm) {
    if (Log.levelMet(Level.INFO)) {
      Log.infof("SimpleRetrievalOperation.replicaExcluded(): %s %s", key, replica);
    }
    
    if (forwardingMode == ForwardingMode.DO_NOT_FORWARD) {
      throw new RuntimeException("Invalid replicaIncluded() invocation");
    }
    
    update(key, replica, new RetrievalResult(key, OpResult.REPLICA_EXCLUDED, null), rComm);
  }
}

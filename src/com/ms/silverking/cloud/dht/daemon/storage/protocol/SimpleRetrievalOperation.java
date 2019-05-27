package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

/**
 * A retrieval operation that expects all primary replicas to be authoritative. 
 * Secondary replicas are not authoritative.  
 */
class SimpleRetrievalOperation extends BaseRetrievalOperation<RetrievalEntrySingleState> {
    SimpleRetrievalOperation(long deadline, 
                             RetrievalOperationContainer retrievalOperationContainer, 
                             ForwardingMode forwardingMode) {
        super(deadline, retrievalOperationContainer, forwardingMode);
    }
    
    @Override
    protected RetrievalEntrySingleState initializeEntryState(DHTKey entryKey, 
                        List<IPAndPort> primaryReplicas, List<IPAndPort> secondaryReplicas) {
        RetrievalEntrySingleState   entryState;
        
        if (debug) {
            System.out.printf("initializeEntryState %s %s %s\n", entryKey, 
                CollectionUtil.toString(primaryReplicas), CollectionUtil.toString(secondaryReplicas));
        }
        entryState = new RetrievalEntrySingleState(primaryReplicas, secondaryReplicas);
        setEntryState(entryKey, entryState);
        return entryState;
    }
    
    @Override
    public void update(DHTKey key, IPAndPort replica, RetrievalResult update, 
                           RetrievalVirtualCommunicator rvComm) {
        RetrievalEntrySingleState   entryState;
        RetrievalState  prevState;
        //OpResult        result;
        
        if (debug) {
            Log.warning("SimpleRetrievalOperation.update(): ", key + " "+ replica +" "+ update);
            //Thread.dumpStack();
        }
        //result = update.getResult();
        
        entryState = getEntryState(key);
        if (entryState == null) {
            if (debug) {
                System.out.printf("null entry state %s %s %s\n", key, replica, update);
            }
            return;
        }
        
        synchronized (entryState) {
            prevState = entryState.getState();
            if (prevState.isComplete()) {
                return; // ignore complete states
            }
            
            // FUTURE - need to handle potentially incomplete success
            // (generated when we get a value that we're not sure about whether a newer value is
            // around)
            // leave out for now
            
            switch (update.getResult()) {
            // FUTURE - need to have timeouts or errors handled
            // trouble isn't handling but generating the events so that this code is called
            case TIMEOUT:// fall through
            case CORRUPT:
                // FIXME - need to mark replica as potentially bad here
                if (forwardingMode == ForwardingMode.DO_NOT_FORWARD) {
                    entryState.setState(RetrievalState.CORRUPT);
                    synchronized (rvComm) {
                        rvComm.sendResult(update);
                    }
                } else {
                    // FUTURE - REDUCE DUPLICATION WITH BELOW CODE
                    IPAndPort   nextReplica;
                    
                    nextReplica = entryState.nextReplica();
                    if (debug) {
                        System.out.printf("forward entry state %s %s\n", key, nextReplica);
                    }
                    if (nextReplica == null) {
                        entryState.setState(RetrievalState.CORRUPT);
                        synchronized (rvComm) {
                            rvComm.sendResult(update);
                        }
                        return;
                    }
                    synchronized (rvComm) {
                        rvComm.forwardEntry(nextReplica, key);
                    }
                    switch (prevState) {
                    case INITIAL:
                    case SECONDARY_NO_SUCH_VALUE: 
                    case SECONDARY_FAILED:
                        break;
                    default: throw new RuntimeException("Unexpected prevState: "+ prevState);
                    }
                }
                break;
            case ERROR: // fall through
            case INCOMPLETE: // fall through
            case NO_SUCH_VALUE:
                int _completeEntries;
                
                if ((entryState.isPrimaryReplica(replica) && forwardingMode != ForwardingMode.ALL) || forwardingMode == ForwardingMode.DO_NOT_FORWARD) {
                    entryState.setState(RetrievalState.NO_SUCH_VALUE);
                    synchronized (rvComm) {
                        rvComm.sendResult(update);
                    }
                    _completeEntries = completeEntries.incrementAndGet();
                    if (_completeEntries >= numEntries) {
                        setOpResult(OpResult.SUCCEEDED);
                    }
                    break;
                } else {
                    IPAndPort   nextReplica;
                    
                    nextReplica = entryState.nextReplica();
                    if (debug) {
                        System.out.printf("forward entry state %s %s\n", key, nextReplica);
                    }
                    if (nextReplica == null) {
                    	if (forwardingMode == ForwardingMode.ALL) {
                            entryState.setState(RetrievalState.NO_SUCH_VALUE);
                            synchronized (rvComm) {
                                rvComm.sendResult(update);
                            }
                            _completeEntries = completeEntries.incrementAndGet();
                            if (_completeEntries >= numEntries) {
                                setOpResult(OpResult.SUCCEEDED);
                            }
                    	}
                        return;
                    }
                    synchronized (rvComm) {
                        rvComm.forwardEntry(nextReplica, key);
                    }
                    switch (prevState) {
                    case INITIAL:
                    case SECONDARY_NO_SUCH_VALUE: 
                    case SECONDARY_FAILED:
                        break;
                    default: throw new RuntimeException("Unexpected prevState: "+ prevState);
                    }
                }
                break;
            case SUCCEEDED: 
                entryState.setState(RetrievalState.SUCCEEDED);
                if (entryState.isPrimaryReplica(replica)) {
                    synchronized (rvComm) {
                        rvComm.sendResult(update, entryState.getSecondaryReplicas());
                    }
                } else {
                    synchronized (rvComm) {
                        rvComm.sendResult(update);
                    }
                }
                _completeEntries = completeEntries.incrementAndGet();
                if (_completeEntries >= numEntries) {
                    setOpResult(OpResult.SUCCEEDED);
                }
                break;
            default: throw new RuntimeException("Unexpected update result: "+ update.getResult());
            }
        }
    }
}

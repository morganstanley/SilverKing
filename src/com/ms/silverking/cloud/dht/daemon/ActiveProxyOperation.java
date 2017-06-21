package com.ms.silverking.cloud.dht.daemon;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.KeyedResult;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.storage.StorageModule;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.OpCommunicator;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.Operation;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.OperationContainer;
import com.ms.silverking.cloud.dht.net.ForwardingMode;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutMessageGroup;
import com.ms.silverking.cloud.dht.net.protocol.KeyValueMessageFormat;
import com.ms.silverking.cloud.toporing.PrimarySecondaryIPListPair;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

/**
 * Base class for proxy operations - operations executed by a DHTNode on behalf of a 
 * client.
 * 
 * This class is used at both the proximal proxy and the terminal DHT node, and potentially
 * other transient DHT nodes.
 * 
 * The enclosed Operation implements the semantics of the storage and retrieval protocols.
 * 
 * The originator is the IP and port of the client.
 */
abstract class ActiveProxyOperation<K extends DHTKey,R extends KeyedResult> implements OperationContainer {
    protected final UUIDBase        uuid;
    protected MessageGroup          message; // not final to allow forwarding to null out to enable gc
    protected final ForwardingMode  forwardingMode;
    protected final long            namespace;
    protected final byte[]          originator;
    protected final MessageModule   messageModule;
    protected final MessageGroupConnectionProxy connection;
    protected final ByteBuffer      optionsByteBuffer;
    protected final long            absDeadlineMillis;
    protected final int             estimatedKeys;
    protected final boolean         sendResultsDuringStart;
    
    // FUTURE - this class assumes keyed operations; probably create a parent without that assumption
    
    protected Operation<K,R>          operation;
    
    protected static final boolean  debug = false;
    
    ActiveProxyOperation(MessageGroupConnectionProxy connection, MessageGroup message,
                         MessageModule messageModule, long absDeadlineMillis, boolean sendResultsDuringStart) {
        this.connection = connection;
        this.message = message;
        forwardingMode = getForwardingMode(message);
        uuid = message.getUUID();
        namespace = message.getContext();
        originator = message.getOriginator();
        this.messageModule = messageModule;
        this.optionsByteBuffer = message.getBuffers()[KeyValueMessageFormat.optionBufferIndex];
        this.absDeadlineMillis = absDeadlineMillis;
        if (debug) {
        	Log.warning("Deadline: ", new java.util.Date(absDeadlineMillis));
        }
        this.estimatedKeys = message.estimatedKeys();
        this.sendResultsDuringStart = sendResultsDuringStart;
    }
    
    protected static ForwardingMode getForwardingMode(MessageGroup message) {
    	return StorageModule.isDynamicNamespace(message.getContext()) ? ForwardingMode.DO_NOT_FORWARD : message.getForwardingMode();
    }
    
    public int getNumEntries() {
        return estimatedKeys;
    }
    
    public boolean hasTimedOut(long curTime) {
        return curTime > absDeadlineMillis;
    }
    
    protected void setOperation(Operation<K,R> operation) {
        this.operation = operation;
    }
    
    protected void startOperation(OpCommunicator<K,R> comm, Iterable<? extends K> iterable, ForwardCreator<K> forwardCreator) {
        Map<IPAndPort,List<K>>  replicaMessageLists;
        
        //if (DebugUtil.delayedDebug()) {
        if (debug) {
            System.out.println("startOperation");
        }
        /*
         * Performs the main work of the operation; then performs any forwarding,
         * and sends back any locally computed results. 
         */
        processInitialMessageEntries(message, comm, iterable);
        replicaMessageLists = comm.takeReplicaMessageLists();
        if (replicaMessageLists != null) {
            forwardGroupedEntries(replicaMessageLists, optionsByteBuffer, forwardCreator, comm);
        }
        if (sendResultsDuringStart) {
            sendResults(comm);
        } else {
            //comm.takeResults();
        }
    }
    
    void sendResults(OpCommunicator<K,R> comm) {
        sendResults(comm.takeResults());
    }
    
    protected abstract void sendResults(List<R> results);
    
    // FUTURE - Consider making this a table lookup to improve speed
    private static final RingOwnerQueryOpType messageTypeToOwnerQueryOpType(MessageType messageType) {
        switch (messageType) {
        case PUT:
            return RingOwnerQueryOpType.Write;
        case RETRIEVE:
            return RingOwnerQueryOpType.Read;
        default: throw new RuntimeException("Unexpected messagetype for conversion: "+ messageType);
        }
    }
    
    protected void processInitialMessageEntries(MessageGroup update, OpCommunicator<K,R> comm, Iterable<? extends K> iterable) {
        if (debug) {
            System.out.println("processInitialMessageEntries()");
        }
        for (K entry : iterable) {
            PrimarySecondaryIPListPair listPair;
            List<IPAndPort>   primaryReplicas;
            List<IPAndPort>   secondaryReplicas;
            
            // Find the replicas for this message entry
            listPair = messageModule.getReplicaListPair(update.getContext(), entry, 
                                                        messageTypeToOwnerQueryOpType(update.getMessageType()));
            primaryReplicas = listPair.getPrimaryOwners();
            if (primaryReplicas.size() == 0) {
            	Log.warning(String.format("No primary replicas found for %s", KeyUtil.keyToString(entry)));
            	throw new RuntimeException("No primary replica found for "+ KeyUtil.keyToString(entry));
            }
            secondaryReplicas = listPair.getSecondaryOwners();
            if (debug) {
                System.out.printf("primaryReplicas.size() %d\n", primaryReplicas.size());
            }
            //operation.processInitialMessageGroupEntry(entry, primaryReplicas, secondaryReplicas, comm);
            processInitialMessageGroupEntry(entry, primaryReplicas, secondaryReplicas, comm);
        }
    }
    
    protected void processInitialMessageGroupEntry(K entry, List<IPAndPort> primaryReplicas, 
                                                      List<IPAndPort> secondaryReplicas, OpCommunicator<K,R> comm) {
        operation.processInitialMessageGroupEntry(entry, primaryReplicas, secondaryReplicas, comm);
    }
    
    /**
     * Forward all groups in the map.
     * @param destEntryMap
     * @param optionsByteBuffer
     */
    protected <L extends DHTKey> void forwardGroupedEntries(Map<IPAndPort, List<L>> destEntryMap,
            ByteBuffer optionsByteBuffer, ForwardCreator<L> forwardCreator, OpCommunicator<K,R> comm) {
        if (debug) {
            System.out.println("forwardGroupedEntries "+ destEntryMap.size());
        }
        for (Map.Entry<IPAndPort, List<L>> entry : destEntryMap.entrySet()) {
            if (debug) {
                System.out.println(entry.getKey());
            }
            forwardGroup(entry.getKey(), entry.getValue(), optionsByteBuffer, forwardCreator, comm);
        }
    }

    /** 
     * Forward to a single replica
     * @param replica
     * @param destEntries
     * @param optionsByteBuffer
     */
    private <L extends DHTKey> void forwardGroup(IPAndPort replica, List<L> destEntries,
            ByteBuffer optionsByteBuffer, ForwardCreator<L> forwardCreator, OpCommunicator<K,R> comm) {
        
        if (forwardingMode.forwards()) {
            MessageGroup mg;
    
            assert replica != null;
            mg = forwardCreator.createForward(destEntries, optionsByteBuffer);
            if (debug) {
                System.out.println("Forwarding: "+ new SimpleValueCreator(originator) + ":" + replica + " : " + mg +":"+ mg.getForwardingMode());
                mg.displayForDebug();
            }
            messageModule.getMessageGroupBase().send(mg, replica);
        } else {
            localOp(destEntries, comm);
        }
    }
    
    protected abstract void localOp(List<? extends DHTKey> destEntries, OpCommunicator<K,R> comm);
    
    protected List<IPAndPort> getFilteredSecondaryReplicas(K entry, List<IPAndPort> primaryReplicas, 
            List<IPAndPort> secondaryReplicas, OpCommunicator<K,R> comm,
            Set<SecondaryTarget> secondaryTargets) {
        List<IPAndPort> filteredSecondaryReplicas;
        
        if (forwardingMode.forwards()) {
            if (secondaryTargets != null) {
                Set<IPAndPort>  secondarySet;
    
                secondarySet = messageModule.getSecondarySet(secondaryTargets);
                if (secondarySet.isEmpty()) {
                    filteredSecondaryReplicas = ImmutableList.of();
                } else {
                    filteredSecondaryReplicas = new ArrayList<>(secondaryReplicas.size());
                    for (IPAndPort replica : secondaryReplicas) {
                        if (secondarySet.contains(replica)) {
                            filteredSecondaryReplicas.add(replica);
                        }
                    }
                }
            } else {
                filteredSecondaryReplicas = secondaryReplicas;
            }
        } else {
            filteredSecondaryReplicas = secondaryReplicas;
        }
        return filteredSecondaryReplicas;
    }
    
    //////////////////////////////////////
    // OperationContainer implementation
    
    public IPAndPort localIPAndPort() {
        return messageModule.localIPAndPort();
    }
    
    public boolean isLocalReplica(IPAndPort replica) {
        return messageModule.isLocal(replica);
    }
    
    public boolean containsLocalReplica(List<IPAndPort> primaryReplicas) {
        return primaryReplicas.contains(messageModule.localIPAndPort());
    }
    
    public StorageModule getStorage() {
        return messageModule.getStorage();
    }
    
    public long getContext() {
        return message.getContext();
    }
        
    public byte[] getValueCreator() {
        return ProtoPutMessageGroup.getValueCreator(message);
    }
    
    public Set<IPAndPort> checkForReplicaTimeouts(long curTimeMillis) {
    	return ImmutableSet.of();
    }
}

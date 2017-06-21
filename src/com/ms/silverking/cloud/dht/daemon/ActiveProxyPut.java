package com.ms.silverking.cloud.dht.daemon;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.OpCommunicator;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.PutCommunicator;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.PutOperationContainer;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageOperation;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageProtocol;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageProtocolUtil;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupKeyOrdinalEntry;
import com.ms.silverking.cloud.dht.net.MessageGroupPutEntry;
import com.ms.silverking.cloud.dht.net.ProtoPutForwardMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutResponseMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoPutUpdateMessageGroup;
import com.ms.silverking.cloud.dht.net.PutResult;
import com.ms.silverking.cloud.dht.net.protocol.PutMessageFormat;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.time.SystemTimeSource;

/**
 * Put executed on behalf of a client. The internal StorageOperation implements the StorageProtocol 
 * specific behavior.
 */
class ActiveProxyPut extends ActiveProxyOperation<MessageGroupKeyEntry, PutResult> implements PutOperationContainer {
    private final StorageOperation storageOperation;
    private final long version;
    private final int   stLength;
    private final Set<SecondaryTarget>  secondaryTargets;

    ActiveProxyPut(MessageGroup message, MessageGroupConnectionProxy connection, MessageModule messageModule,
            StorageProtocol storageProtocol, long absDeadlineMillis, boolean local, NamespaceOptions nsOptions) {
        super(connection, message, messageModule, absDeadlineMillis, local || storageProtocol.sendResultsDuringStart());
        long    _version;
        SystemTimeSource    systemTimeSource;
        
        systemTimeSource = SystemTimeUtil.systemTimeSource;
        _version = ProtoPutMessageGroup.getPutVersion(message);
        if (_version == DHTConstants.unspecifiedVersion) {
            switch (nsOptions.getVersionMode()) {
            case SYSTEM_TIME_MILLIS: _version = systemTimeSource.absTimeMillis(); break;
            case SEQUENTIAL: // FIXME - for now we just use nanos for sequential version
            case SYSTEM_TIME_NANOS: _version = systemTimeSource.absTimeNanos(); break;
            default: throw new RuntimeException("non-system or unexpected VersionMode: "+ nsOptions.getVersionMode());
            }
            ProtoPutMessageGroup.setPutVersion(message, _version);
        }
        version = _version;
        
        stLength = ProtoPutMessageGroup.getSTLength(message);
        storageOperation = storageProtocol
                .createStorageOperation(message.getDeadlineAbsMillis(messageModule.getAbsMillisTimeSource()), this,
                        message.getForwardingMode());
        super.setOperation(storageOperation);
        secondaryTargets = _getSecondaryTargets(message);
    }
    
    void startOperation() {
        PutCommunicator pComm;
        
        if (debug) {
            System.out.println(this +" "+ forwardingMode +" "+ storageOperation);
        }
        pComm = new PutCommunicator(this);
        if (forwardingMode.forwards()) {
            messageModule.addActivePut(uuid, this);
        }
        super.startOperation(pComm, 
                message.getPutValueKeyIterator(ProtoPutMessageGroup.getChecksumType(message)),
                new PutForwardCreator());
        // super.startOperation() will forward initial puts. Now we need to forward any
        // state updates created due to local processing.
        forwardGroupedEntries(pComm.takeReplicaUpdateMessageLists(), optionsByteBuffer, 
                new PutUpdateForwardCreator(storageOperation.nextStorageState(storageOperation.initialStorageState())),
                pComm);
        message = null; // free payload for GC
    }

    protected void processInitialMessageGroupEntry(MessageGroupKeyEntry entry, List<IPAndPort> primaryReplicas, 
            List<IPAndPort> secondaryReplicas, OpCommunicator<MessageGroupKeyEntry,PutResult> comm) {
        List<IPAndPort> filteredSecondaryReplicas;
        
        filteredSecondaryReplicas = getFilteredSecondaryReplicas(entry, primaryReplicas, secondaryReplicas, 
                                                                 comm, secondaryTargets);
        super.processInitialMessageGroupEntry(entry, primaryReplicas, filteredSecondaryReplicas, comm);
    }
    
    class PutForwardCreator implements ForwardCreator<MessageGroupKeyEntry> {
        @Override
        public MessageGroup createForward(List<MessageGroupKeyEntry> destEntries, ByteBuffer optionsByteBuffer) {
            ProtoPutForwardMessageGroup protoMG;
    
            protoMG = new ProtoPutForwardMessageGroup(uuid, namespace, originator, optionsByteBuffer, destEntries,
                    PutMessageFormat.getChecksumType(optionsByteBuffer),
                    messageModule.getAbsMillisTimeSource().relMillisRemaining(absDeadlineMillis));
            return protoMG.toMessageGroup();
        }
    }

    class PutUpdateForwardCreator implements ForwardCreator<MessageGroupKeyOrdinalEntry> {
        private final byte  storageState;
        
        public PutUpdateForwardCreator(byte storageState) {
            this.storageState = storageState;
        }
        
        @Override
        public MessageGroup createForward(List<MessageGroupKeyOrdinalEntry> destEntries, ByteBuffer optionsByteBuffer) {
            ProtoPutUpdateMessageGroup protoMG;
    
            protoMG = new ProtoPutUpdateMessageGroup(uuid, namespace,  
                            version, destEntries, originator, storageState, 
                            messageModule.getAbsMillisTimeSource().relMillisRemaining(absDeadlineMillis));
            return protoMG.toMessageGroup();
        }
    }
    
    protected void localOp(List<? extends DHTKey> _entries, OpCommunicator<MessageGroupKeyEntry, PutResult> comm) {
        boolean useUpdate;
        PutCommunicator pComm;
        
        pComm = (PutCommunicator)comm;
        useUpdate = _entries.size() > 0 && (_entries.get(0) instanceof MessageGroupKeyOrdinalEntry);
        if (!useUpdate) {
            List<StorageValueAndParameters> values;
            long    creationTime;
            
            creationTime = SystemTimeUtil.systemTimeSource.absTimeNanos();
            values = new ArrayList<>(_entries.size());
            for (DHTKey _entry : _entries) {
                values.add(new StorageValueAndParameters((MessageGroupPutEntry)_entry, (PutOperationContainer)this, 
                                                        creationTime));
                if (debug) {
                    System.out.printf("localOp: %s\n", _entry);
                }
                //Log.fine(entry);
            }
            getStorage().put(
                        getContext(),
                        values,
                        getUserData(),
                        pComm);
            if (forwardingMode.forwards()) {
                for (DHTKey _entry : _entries) {
                    storageOperation.localUpdate(_entry, 
                                                StorageProtocolUtil.initialStorageStateOrdinal, 
                                                OpResult.SUCCEEDED, pComm);
                }
            }
        } else {
            localOp_putupdate(_entries, pComm);
        }
    }
    
    protected void localOp_putupdate(List<? extends DHTKey> _entries, PutCommunicator pComm) {
        List<OpResult>  results;
        int _entriesSize;
        
        results = getStorage().putUpdate(getContext(), _entries, getVersion());
        _entriesSize = _entries.size();
        if (results.size() != _entriesSize) {
            throw new RuntimeException("panic");
        }
        for (int i = 0; i < _entriesSize; i++) {
            DHTKey                      _entry;
            MessageGroupKeyOrdinalEntry mgkoEntry;
            OpResult                    result;
            
            _entry = _entries.get(i);
            if (debug) {
                System.out.printf("localOp_putupdate: %s\n", _entry);
            }
            mgkoEntry = (MessageGroupKeyOrdinalEntry)_entry;
            result = results.get(i);
            storageOperation.localUpdate(_entry, 
                                        mgkoEntry.getOrdinal(), 
                                        result, pComm);
        }
    }
    
    // ///////////////////
    // handle responses

    /**
     * Process a put response according to the protocol in use by this StorageOperation.
     * 
     * @param message
     * @param _connection
     * @return 
     */
    OpResult handlePutResponse(MessageGroup message, MessageGroupConnectionProxy _connection) {
        PutCommunicator pComm;
        byte            storageState;

        pComm = new PutCommunicator(this);
        if (debug) {
            System.out.println("handlePutResponse");
        }
        storageState = ProtoPutResponseMessageGroup.getStorageState(message);
        for (MessageGroupKeyOrdinalEntry entry : message.getKeyOrdinalIterator()) {
            IPAndPort replica;

            replica = new IPAndPort(message.getOriginator(), DHTNode.getServerPort());
            if (debug) {
                System.out.println("replica: " + replica);
            }
            storageOperation.update(entry.getKey(), replica, storageState, EnumValues.opResult[entry.getOrdinal()], pComm);
        }
        
        // forward state updates
        Map<IPAndPort, List<MessageGroupKeyOrdinalEntry>>   rumLists;
        
        rumLists = pComm.takeReplicaUpdateMessageLists();
        if (rumLists.size() > 0) {
            forwardGroupedEntries(rumLists, optionsByteBuffer, 
                          new PutUpdateForwardCreator(storageOperation.nextStorageState(storageState)),
                          pComm);
        }
        
        // send responses for completions
        messageModule.sendPutResults(message, version, connection, pComm.takeResults(), storageState, 
                message.getDeadlineRelativeMillis());
        return storageOperation.getOpResult();
    }
    
    /**
     * sendResults on initial operation startup
     */
    @Override
    protected void sendResults(List<PutResult> results) {
        // FUTURE - remove this method from ActiveOperation?
        messageModule.sendPutResults(message, version, connection, results, StorageProtocolUtil.initialStorageStateOrdinal, 
                message.getDeadlineRelativeMillis());
    }    
    
    /////////////////////////////////////////
    // PutOperationContainer implementation
    
    public long getVersion() {
        return ProtoPutMessageGroup.getPutVersion(message);
    }
    
    public byte[] getUserData() {
        return ProtoPutMessageGroup.getUserData(message, stLength);
    }
    
    public short getCCSS() {
        return ProtoPutMessageGroup.getCCSS(message);
    }    
    
    public OpResult getOpResult() {
        return storageOperation.getOpResult();
    }
    
    private static Set<SecondaryTarget> _getSecondaryTargets(MessageGroup mg) {
        return ProtoPutMessageGroup.getSecondaryTargets(mg);
    }
    
    public Set<SecondaryTarget> getSecondaryTargets() {
        return secondaryTargets;
    }    
}

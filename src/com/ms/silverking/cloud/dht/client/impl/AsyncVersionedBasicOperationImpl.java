package com.ms.silverking.cloud.dht.client.impl;

import java.util.List;

import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.common.Context;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoVersionedBasicOpMessageGroup;

public abstract class AsyncVersionedBasicOperationImpl extends AsyncNamespaceOperationImpl {
    private final VersionedBasicNamespaceOperation  versionedOperation;
    
    private static final boolean    debug = false;

    public AsyncVersionedBasicOperationImpl(VersionedBasicNamespaceOperation versionedOperation, Context context, long curTime, byte[] originator) {
        super(versionedOperation, context, curTime, originator);
        this.versionedOperation = versionedOperation;
    }
    
    @Override 
	protected NonExistenceResponse getNonExistenceResponse() {
    	return null;
    }

    @Override
    public void waitForCompletion() throws OperationException {
        super._waitForCompletion();
    }

    @Override
    protected int opWorkItems() {
        return 1;
    }

    @Override
    void addToEstimate(MessageEstimate estimate) {
    }

    @Override
    MessageEstimate createMessageEstimate() {
        return null;
    }
    
    // FUTURE - think about moving this
    private static MessageType clientOpTypeToMessageType(ClientOpType clientOpType) {
        switch (clientOpType) {
        case SNAPSHOT: return MessageType.SNAPSHOT;
        case SYNC_REQUEST: return MessageType.SYNC_REQUEST;
        default: throw new RuntimeException("Unsupported clientOpType: "+ clientOpType);
        }
    }

    @Override
    ProtoMessageGroup createProtoMG(MessageEstimate estimate) {
        return new ProtoVersionedBasicOpMessageGroup(clientOpTypeToMessageType(operation.getOpType()), 
                    operation.getUUID(), context.contextAsLong(), versionedOperation.getVersion(), originator);
    }

    @Override
    ProtoMessageGroup createMessagesForIncomplete(ProtoMessageGroup protoMG, List<MessageGroup> messageGroups,
            MessageEstimate estimate) {
        if (debug) {
            System.out.println("createMessagesForIncomplete");
            System.out.println(messageGroups.size());
        }
        ((ProtoVersionedBasicOpMessageGroup)protoMG).setNonEmpty();
        protoMG.addToMessageGroupList(messageGroups);
        return createProtoMG(estimate);
    }

}

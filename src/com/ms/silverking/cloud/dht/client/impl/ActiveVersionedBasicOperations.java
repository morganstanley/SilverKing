package com.ms.silverking.cloud.dht.client.impl;

import java.lang.ref.WeakReference;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

import org.hibernate.validator.internal.util.ConcurrentReferenceHashMap;


class ActiveVersionedBasicOperations {
    private final ConcurrentMap<UUIDBase,WeakReference<AsyncVersionedBasicOperationImpl>>   activeOps;

    ActiveVersionedBasicOperations() {
        activeOps = new ConcurrentReferenceHashMap<>();      
    }
    
    Set<AsyncVersionedBasicOperationImpl> currentOpSet() {
        ImmutableSet.Builder<AsyncVersionedBasicOperationImpl>  setBuilder;

        setBuilder = new ImmutableSet.Builder<>();
        for (WeakReference<AsyncVersionedBasicOperationImpl> opRef : activeOps.values()) {
            AsyncVersionedBasicOperationImpl    op;
            
            op = opRef.get();
            if (op != null) {
                setBuilder.add(op);
            }
        }
        return setBuilder.build();
    }
    
    public void addOp(AsyncVersionedBasicOperationImpl op) {
        activeOps.put(op.getUUID(), new WeakReference<>(op));
    }
    
    private AsyncVersionedBasicOperationImpl getOp(UUIDBase uuid) {
        WeakReference<AsyncVersionedBasicOperationImpl> ref;
        ref = activeOps.get(uuid);
        if (ref == null) {
            return null;
        } else {
            return ref.get();
        }
    }
    
    public void receivedOpResponse(MessageGroup message) {
        AsyncVersionedBasicOperationImpl    op;
        UUIDBase    uuid;
        byte        resultCode;
        long        uuidMSL;
        long        uuidLSL;
        
        uuidMSL = message.getBuffers()[0].getLong(0);
        uuidLSL = message.getBuffers()[0].getLong(NumConversion.BYTES_PER_LONG);
        resultCode = message.getBuffers()[0].get(2 * NumConversion.BYTES_PER_LONG);
        uuid = new UUIDBase(uuidMSL, uuidLSL);
        op = getOp(uuid);
        if (op != null) {
            OpResult    result;
            
            result = EnumValues.opResult[resultCode];
            op.setResult(result);
        } else {
            Log.warning("No operation for response: ", uuid);
        }
    }
}

package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.net.protocol.KeyedMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalMessageFormat;
import com.ms.silverking.cloud.dht.net.protocol.RetrievalResponseMessageFormat;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;

public class ProtoRetrievalMessageGroup extends ProtoKeyedMessageGroup {
    private static final int  optionBufferIndex = 1;
    
    public ProtoRetrievalMessageGroup(UUIDBase uuid, long context, InternalRetrievalOptions retrievalOptions,
            byte[] originator, int size, int deadlineRelativeMillis, ForwardingMode forward) {
        super(MessageType.RETRIEVE, uuid, context,
                ByteBuffer.allocate(RetrievalMessageFormat.getOptionsBufferLength(retrievalOptions.getRetrievalOptions())),
                size, RetrievalMessageFormat.size - KeyedMessageFormat.baseBytesPerKeyEntry, 
                originator, deadlineRelativeMillis, forward);
        Set<SecondaryTarget>   secondaryTargets;
        
        // FUTURE - merge with ProtoValueMessagGroup code?
        bufferList.add(optionsByteBuffer);
        // begin retrievalType, waitMode encoding
            // see getRetrievalOptions() for decoding
        optionsByteBuffer.put((byte) ((retrievalOptions.getRetrievalType().ordinal() << 4) | retrievalOptions
                .getWaitMode().ordinal()));
        // end retrievalType, waitMode encoding
        optionsByteBuffer.put((byte) (((retrievalOptions.getVerifyIntegrity() ? 1 : 0) << 1)
                            | (retrievalOptions.getRetrievalOptions().getUpdateSecondariesOnMiss() ? 1 : 0)) );
        VersionConstraint vc = retrievalOptions.getVersionConstraint();
        optionsByteBuffer.putLong(vc.getMin());
        optionsByteBuffer.putLong(vc.getMax());
        optionsByteBuffer.put((byte) vc.getMode().ordinal());
        optionsByteBuffer.putLong(vc.getMaxCreationTime());
        secondaryTargets = retrievalOptions.getRetrievalOptions().getSecondaryTargets();
        if (secondaryTargets == null) {
            optionsByteBuffer.putShort((short)0);
        } else {
            byte[]  serializedST;
            
            serializedST = SecondaryTargetSerializer.serialize(secondaryTargets);
            optionsByteBuffer.putShort((short)serializedST.length);
            optionsByteBuffer.put(serializedST);
        }
    }
    
    /*
    public ProtoRetrievalMessageGroup(UUIDBase uuid, long context, RetrievalOptions retrievalOptions,
            byte[] originator, List<? extends DHTKey> destEntries, int deadlineRelativeMillis) {
        this(uuid, context, retrievalOptions, originator, destEntries.size(), 
                deadlineRelativeMillis, ForwardingMode.DO_NOT_FORWARD);
        for (DHTKey key : destEntries) {
            addKey(key);
        }
    }
    */
    
    public ProtoRetrievalMessageGroup(UUIDBase uuid, long context, InternalRetrievalOptions retrievalOptions,
            byte[] originator, Collection<DHTKey> keys, int deadlineRelativeMillis) {
        this(uuid, context, retrievalOptions, originator, keys.size(), 
                deadlineRelativeMillis, ForwardingMode.DO_NOT_FORWARD);
        for (DHTKey key : keys) {
            addKey(key);
        }
    }
    
    public static InternalRetrievalOptions getRetrievalOptions(MessageGroup mg) {
        int                 retrievalWaitByte;
        RetrievalType       retrievalType;
        WaitMode            waitMode;
        int                 miscOptionsByte;
        VersionConstraint   vc;
        ByteBuffer          optionBuffer;
        boolean             verifyIntegrity;
        boolean             updateSecondariesOnMiss;
        
        optionBuffer = mg.getBuffers()[optionBufferIndex];
        retrievalWaitByte = optionBuffer.get(RetrievalResponseMessageFormat.retrievalTypeWaitModeOffset);
        // begin retrievalType, waitMode decoding
            // see ProtoRetrievalMessageGroup() for encoding
        retrievalType = EnumValues.retrievalType[retrievalWaitByte >> 4];
        waitMode = EnumValues.waitMode[retrievalWaitByte & 0x0f];
        // end retrievalType, waitMode decoding
        miscOptionsByte = optionBuffer.get(RetrievalResponseMessageFormat.miscOptionsOffset);
        verifyIntegrity = (miscOptionsByte & 0x2) != 0;
        updateSecondariesOnMiss = (miscOptionsByte & 0x1) != 0;
        vc = new VersionConstraint(optionBuffer.getLong(RetrievalResponseMessageFormat.vcMinOffset), 
                       optionBuffer.getLong(RetrievalResponseMessageFormat.vcMaxOffset),
                       EnumValues.versionConstraint_Mode[optionBuffer.get(RetrievalResponseMessageFormat.vcModeOffset)],
                       optionBuffer.getLong(RetrievalResponseMessageFormat.vcMaxStorageTimeOffset));
        return new InternalRetrievalOptions(OptionsHelper.newRetrievalOptions(retrievalType, waitMode, 
                                                vc, updateSecondariesOnMiss, getSecondaryTargets(mg)), verifyIntegrity);
    }
    
    public static int getSTLength(MessageGroup mg) {
        return mg.getBuffers()[optionBufferIndex].getShort(RetrievalMessageFormat.stDataOffset);
    }
    
    private static Set<SecondaryTarget> getSecondaryTargets(MessageGroup mg) {
        int     stLength;
        byte[]  stDef;
        
        stLength = getSTLength(mg);
        if (stLength == 0) {
            return DHTConstants.noSecondaryTargets;
        } else {
            stDef = new byte[stLength];
            System.arraycopy(mg.getBuffers()[optionBufferIndex].array(), 
                    RetrievalMessageFormat.stDataOffset + NumConversion.BYTES_PER_SHORT, stDef, 0, stLength);
            return SecondaryTargetSerializer.deserialize(stDef);
        }
    }
    
    protected MessageGroup toMessageGroup(boolean flip) {
        MessageGroup    mg;
        
        mg = super.toMessageGroup(flip);
        return mg;
    }
}

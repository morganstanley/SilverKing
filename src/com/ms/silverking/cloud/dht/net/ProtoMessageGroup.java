package com.ms.silverking.cloud.dht.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.StringUtil;

/**
 * ProtoMessageGroup and its descendants are used to create actual MessageGroup instances.
 */
public abstract class ProtoMessageGroup {
    private final MessageType         type;
    private final int                 options;
    private final UUIDBase            uuid;
    protected final long              context;
    private final byte[]              originator;
    protected final List<ByteBuffer>  bufferList;
    private final int                 deadlineRelativeMillis;
    private final ForwardingMode      forward;
    
    protected static final boolean  debug = false;
    
    private static final int    bufferListInitialSize = 4;
    
    public ProtoMessageGroup(MessageType type, UUIDBase uuid, long context, 
                             byte[] originator, int deadlineRelativeMillis, ForwardingMode forward) {
        this.type = type;
        this.options = 0; // options are currently only used between peers; set to zero here
        this.uuid = uuid;
        this.context = context;
        assert originator != null && originator.length == ValueCreator.BYTES;
        this.originator = originator;
        this.deadlineRelativeMillis = deadlineRelativeMillis;
        this.forward = forward;
        bufferList = new ArrayList<>(bufferListInitialSize);        
    }
    
    public UUIDBase getUUID() {
        return uuid;
    }
    
    public final List<ByteBuffer> getBufferList() {
        return bufferList;
    }

    public abstract boolean isNonEmpty();
    
    public MessageGroup toMessageGroup() {
        return toMessageGroup(true);
    }
    
    protected MessageGroup toMessageGroup(boolean flip) {
        MessageGroup    mg;
        
        if (debug) {
            System.out.println("toMessageGroup: "+ flip);
            displayForDebug();
        }
        mg = new MessageGroup(type, options, uuid, context, flip ? BufferUtil.flip(getBufferList()) : getBufferList(), 
                                originator, deadlineRelativeMillis, forward);
        if (debug) {
            mg.displayForDebug();
        }
        return mg;
    }
        
    protected void displayForDebug() {
        for (int i = 0; i < bufferList.size(); i++) {
            System.out.println(i +"\t"+ bufferList.get(i));
            if (bufferList.get(i).limit() < 128) {
                System.out.println(StringUtil.byteBufferToHexString((ByteBuffer)bufferList.get(i).duplicate().position(0)));
            } else {
                System.out.println();
            }
        }
    }
    
    /**
     * Convert this ProtoMessageGroup to a MessageGroup and add it to the given list
     * @param messageGroups list to add this to
     */
    public void addToMessageGroupList(List<MessageGroup> messageGroups) {
        if (isNonEmpty()) {
            MessageGroup    messageGroup;

            messageGroup = toMessageGroup();
            messageGroups.add(messageGroup);
            if (Log.levelMet(Level.FINE)) {
                messageGroup.displayForDebug();
            }
        }        
    }    
}

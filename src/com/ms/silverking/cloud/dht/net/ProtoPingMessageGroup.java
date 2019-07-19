package com.ms.silverking.cloud.dht.net;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;

public class ProtoPingMessageGroup extends ProtoVersionedBasicOpMessageGroup {
    public ProtoPingMessageGroup(byte[] originator) {
        super(MessageType.OP_PING, new UUIDBase(), 0, 0, originator);
    }
    
    @Override
    public boolean isNonEmpty() {
        return true;
    }
}

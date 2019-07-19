package com.ms.silverking.cloud.dht.net;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;

public class ProtoPingAckMessageGroup extends ProtoVersionedBasicOpMessageGroup {
    public ProtoPingAckMessageGroup(byte[] originator, UUIDBase uuid) {
        super(MessageType.OP_PING_ACK, uuid, 0, 0, originator);
    }

    @Override
    public boolean isNonEmpty() {
        return true;
    }
}

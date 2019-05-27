package com.ms.silverking.cloud.dht.net;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.id.UUIDBase;

public class ProtoNopMessageGroup extends ProtoVersionedBasicOpMessageGroup {
    public ProtoNopMessageGroup(byte[] originator) {
        super(MessageType.OP_NOP, new UUIDBase(), 0, 0, originator);
    }

    @Override
    public boolean isNonEmpty() {
        return true;
    }
}

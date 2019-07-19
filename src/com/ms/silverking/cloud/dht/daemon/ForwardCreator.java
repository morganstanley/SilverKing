package com.ms.silverking.cloud.dht.daemon;

import java.nio.ByteBuffer;
import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.net.MessageGroup;

public interface ForwardCreator<K extends DHTKey> {
    public MessageGroup createForward(List<K> destEntries, ByteBuffer optionsByteBuffer);
}

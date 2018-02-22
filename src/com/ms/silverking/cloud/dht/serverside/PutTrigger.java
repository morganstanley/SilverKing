package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;

public interface PutTrigger extends Trigger {
    public OpResult put(SSNamespaceStore nsStore, DHTKey key, ByteBuffer value, SSStorageParameters storageParams, byte[] userData, NamespaceVersionMode nsVersionMode);
	public OpResult putUpdate(SSNamespaceStore nsStore, DHTKey key, long version, byte storageState);
}

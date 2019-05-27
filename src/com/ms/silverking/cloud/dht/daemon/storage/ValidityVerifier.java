package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageProtocolUtil;

/**
 * Verify storage state. Possibly also verify integrity.
 * This class is designed to be called out of the critical path in the uncommon
 * instance where storage state of a previous retrieval was found to be bad.
 */
class ValidityVerifier {
	private final ByteBuffer	buf;
    private final ConsistencyProtocol   consistencyProtocol;
	
	ValidityVerifier(ByteBuffer buf, ConsistencyProtocol consistencyProtocol) {
		this.buf = buf;
		this.consistencyProtocol = consistencyProtocol;
	}
	
	boolean isValid(int offset) {
		return StorageProtocolUtil.storageStateValidForRead(consistencyProtocol, 
                						MetaDataUtil.getStorageState(buf, offset));
	}
}

package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageFormat;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;

public class SSUtil {
	private static final byte[]	emptyUserData = new byte[0];
	
	public static ByteBuffer retrievalResultBufferFromValue(byte[] value, SSStorageParameters storageParams) {
		return retrievalResultBufferFromValue(ByteBuffer.wrap(value), storageParams);
	}
	
	public static ByteBuffer retrievalResultBufferFromValue(ByteBuffer value, SSStorageParameters storageParams) {
		ByteBuffer	buf;
		
		buf = ByteBuffer.allocate(MetaDataUtil.computeStoredLength(storageParams.getCompressedSize(), storageParams.getChecksumType().length(), emptyUserData.length));
		StorageFormat.writeToBuf(null, value, StorageParameters.fromSSStorageParameters(storageParams), emptyUserData, buf, new AtomicInteger(0), Integer.MAX_VALUE);
		buf.position(0);
		return buf;
	}
}

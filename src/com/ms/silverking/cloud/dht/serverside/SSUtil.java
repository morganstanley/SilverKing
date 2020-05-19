package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageFormat;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;

public class SSUtil {
  private static final byte[] emptyUserData = new byte[0];
  private static final ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[0]);

  public static ByteBuffer retrievalResultBufferFromValue(byte[] value, SSStorageParameters storageParams,
      SSRetrievalOptions options) {
    return retrievalResultBufferFromValue(ByteBuffer.wrap(value), storageParams, options);
  }

  public static ByteBuffer retrievalResultBufferFromValue(ByteBuffer value, SSStorageParameters storageParams,
      SSRetrievalOptions options) {
    ByteBuffer buf;
    boolean returnValue;

    returnValue = options.getRetrievalType().hasValue();
    buf = ByteBuffer.allocate(MetaDataUtil.computeStoredLength(returnValue ? storageParams.getCompressedSize() : 0,
        storageParams.getChecksumType().length(), emptyUserData.length));
    StorageFormat.writeToBuf(null, value, StorageParameters.fromSSStorageParameters(storageParams), emptyUserData, buf,
        new AtomicInteger(0), Integer.MAX_VALUE, returnValue);
    buf.position(0);
    return buf;
  }

  public static byte[] rawValueToStoredValue(byte[] rawValue, SSStorageParameters storageParams) {
    ByteBuffer buf;

    buf = ByteBuffer.allocate(
        MetaDataUtil.computeStoredLength(storageParams.getCompressedSize(), storageParams.getChecksumType().length(),
            emptyUserData.length));
    StorageFormat.writeToBuf(null, ByteBuffer.wrap(rawValue), StorageParameters.fromSSStorageParameters(storageParams),
        emptyUserData, buf, new AtomicInteger(0), Integer.MAX_VALUE, true);
    return buf.array();
  }

  public static byte[] metaDataToStoredValue(SSStorageParameters storageParams) {
    ByteBuffer buf;

    buf = ByteBuffer.allocate(
        MetaDataUtil.computeMetaDataLength(storageParams.getCompressedSize(), storageParams.getChecksumType().length(),
            emptyUserData.length));
    StorageFormat.writeToBuf(null, emptyBuffer, StorageParameters.fromSSStorageParameters(storageParams), emptyUserData,
        buf, new AtomicInteger(0), Integer.MAX_VALUE, true);
    return buf.array();
  }
}

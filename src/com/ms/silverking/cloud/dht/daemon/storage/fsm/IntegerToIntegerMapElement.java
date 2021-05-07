package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.collection.cuckoo.CuckooBase;
import com.ms.silverking.collection.cuckoo.CuckooConfig;
import com.ms.silverking.collection.cuckoo.IntArrayCuckoo;
import com.ms.silverking.collection.cuckoo.IntBufferCuckoo;
import com.ms.silverking.collection.cuckoo.WritableCuckooConfig;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

public class IntegerToIntegerMapElement extends LTVElement {
  public IntegerToIntegerMapElement(ByteBuffer buf) {
    super(buf);
  }

  public CuckooBase<Integer> getIntegerToIntegerMap() {
    ByteBuffer rawHTBuf;
    ByteBuffer htBuf;
    WritableCuckooConfig segmentCuckooConfig;
    CuckooBase<Integer> integerToOffset;
    int htBufSize;
    int htTotalEntries;

    rawHTBuf = getValueBuffer();
    // FUTURE - cache the below number or does the segment cache do this well enough? (also cache the
    // segmentCuckooConfig...)
    htBufSize = rawHTBuf.getInt(0);
    try {
      htBuf = BufferUtil.sliceAt(rawHTBuf, NumConversion.BYTES_PER_INT + CuckooConfig.BYTES);
    } catch (RuntimeException re) {
      Log.warning(rawHTBuf);
      throw re;
    }

    rawHTBuf = rawHTBuf.order(ByteOrder.nativeOrder());

    htTotalEntries = htBufSize / (NumConversion.BYTES_PER_INT + NumConversion.BYTES_PER_INT);
    segmentCuckooConfig = new WritableCuckooConfig(CuckooConfig.read(rawHTBuf, NumConversion.BYTES_PER_INT),
        -1);
    segmentCuckooConfig = segmentCuckooConfig.newTotalEntries(htTotalEntries);

    integerToOffset = new IntBufferCuckoo(segmentCuckooConfig, htBuf);
    return integerToOffset;
  }

  ////////////////////////////////////

  /*
   * Length   (4)
   * Type     (4)
   * Value:
   * HTLength (4)
   * CuckooConfig (CuckooConfig.BYTES)
   */

  public static IntegerToIntegerMapElement create(IntArrayCuckoo integerToOffset, FSMElementType type) {
    ByteBuffer elementBuf;
    byte[] elementArray;
    int mapSize;
    int headerSize;
    int elementSize;
    int legacyPersistedSize;

    mapSize = integerToOffset.persistedSizeBytes();

    legacyPersistedSize = NumConversion.BYTES_PER_INT + CuckooConfig.BYTES + mapSize;
    elementSize = NumConversion.BYTES_PER_INT * 2   // length + type
        + legacyPersistedSize;
    headerSize = elementSize - mapSize;

    elementArray = integerToOffset.getAsBytesWithHeader(headerSize);
    elementBuf = ByteBuffer.wrap(elementArray);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    elementBuf.putInt(elementSize);
    elementBuf.putInt(type.ordinal());

    elementBuf.putInt(mapSize);
    integerToOffset.getConfig().persist(elementBuf, elementBuf.position());
    elementBuf.position(elementSize);

    elementBuf.rewind();

    return new IntegerToIntegerMapElement(elementBuf);
  }
}

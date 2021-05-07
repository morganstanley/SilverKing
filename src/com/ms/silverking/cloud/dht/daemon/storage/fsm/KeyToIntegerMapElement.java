package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.cloud.dht.collection.DHTKeyCuckooBase;
import com.ms.silverking.collection.cuckoo.CuckooConfig;
import com.ms.silverking.cloud.dht.collection.IntArrayDHTKeyCuckoo;
import com.ms.silverking.cloud.dht.collection.IntBufferDHTKeyCuckoo;
import com.ms.silverking.collection.cuckoo.WritableCuckooConfig;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

public class KeyToIntegerMapElement extends LTVElement {
  private static final boolean debugPersistence = false;

  public KeyToIntegerMapElement(ByteBuffer buf) {
    super(buf);
  }

  public DHTKeyCuckooBase getKeyToIntegerMap() {
    ByteBuffer rawHTBuf;
    ByteBuffer htBuf;
    WritableCuckooConfig segmentCuckooConfig;
    DHTKeyCuckooBase keyToOffset;
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

    htTotalEntries = htBufSize / (NumConversion.BYTES_PER_LONG * 2 + NumConversion.BYTES_PER_INT);
    segmentCuckooConfig = new WritableCuckooConfig(CuckooConfig.read(rawHTBuf, NumConversion.BYTES_PER_INT),
        -1); // FIXME - verify -1
    segmentCuckooConfig = segmentCuckooConfig.newTotalEntries(htTotalEntries);

    keyToOffset = new IntBufferDHTKeyCuckoo(segmentCuckooConfig, htBuf);
    return keyToOffset;
  }

  ////////////////////////////////////

  /*
   * Length   (4)
   * Type     (4)
   * Value:
   * HTLength (4)
   * CuckooConfig (CuckooConfig.BYTES)
   */

  public static KeyToIntegerMapElement create(DHTKeyCuckooBase keyToOffset) {
    ByteBuffer elementBuf;
    byte[] elementArray;
    int mapSize;
    int headerSize;
    int elementSize;
    int legacyPersistedSize;

    mapSize = ((IntArrayDHTKeyCuckoo) keyToOffset).persistedSizeBytes();

    // FIXME
    legacyPersistedSize = NumConversion.BYTES_PER_INT + CuckooConfig.BYTES + mapSize;
    elementSize = NumConversion.BYTES_PER_INT * 2   // length + type
        + legacyPersistedSize;
    headerSize = elementSize - mapSize;

    elementArray = ((IntArrayDHTKeyCuckoo) keyToOffset).getAsBytesWithHeader(headerSize);
    elementBuf = ByteBuffer.wrap(elementArray);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    elementBuf.putInt(elementSize);
    elementBuf.putInt(FSMElementType.OffsetMap.ordinal());

    elementBuf.putInt(mapSize);
    keyToOffset.getConfig().persist(elementBuf, elementBuf.position());
    elementBuf.position(elementSize);

    elementBuf.rewind();

    return new KeyToIntegerMapElement(elementBuf);
  }
}

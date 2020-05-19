package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.cloud.dht.collection.CuckooBase;
import com.ms.silverking.cloud.dht.collection.CuckooConfig;
import com.ms.silverking.cloud.dht.collection.IntArrayCuckoo;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;

/**
 * Mimics LTV storage format for legacy storage.
 */
public class KeyToOffsetMapElementV0 extends KeyToOffsetMapElement {
  static final int legacyLengthOffset = 0;

  static final int headerSize = NumConversion.BYTES_PER_INT + CuckooConfig.BYTES;

  private KeyToOffsetMapElementV0(ByteBuffer buf) {
    super(buf);
    //System.out.printf("buf %s\n", buf);
  }

  public static KeyToOffsetMapElementV0 fromFSMBuffer(ByteBuffer fsmBuf) {
    int htBufSize;
    int legacyPersistedSize;

    htBufSize = fsmBuf.getInt(KeyToOffsetMapElementV0.legacyLengthOffset);
    legacyPersistedSize = headerSize + htBufSize;
    //System.out.printf("htBufSize %d\n", htBufSize);
    return new KeyToOffsetMapElementV0((ByteBuffer) BufferUtil.duplicate(fsmBuf).limit(legacyPersistedSize));
  }

  public int getLegacyPersistedSize() {
    return buf.getInt(legacyLengthOffset);
  }

  public int getLength() {
    return getLegacyPersistedSize() + NumConversion.BYTES_PER_INT * 2;
  }

  public int getType() {
    return FSMElementType.OffsetMap.ordinal();
  }

  /**
   * Unsupported as the underlying storage is not LTV
   */
  public ByteBuffer getBuffer() {
    throw new UnsupportedOperationException();
  }

  public ByteBuffer getValueBuffer() {
    return BufferUtil.duplicate(buf);
  }

  /*
   * Value:
   * HTLength (4)
   * CuckooConfig (CuckooConfig.BYTES)
   */

  public static KeyToOffsetMapElementV0 create(CuckooBase keyToOffset) {
    ByteBuffer elementBuf;
    byte[] elementArray;
    int mapSize;
    int legacyPersistedSize;

    mapSize = ((IntArrayCuckoo) keyToOffset).persistedSizeBytes();

    legacyPersistedSize = headerSize + mapSize;

    elementArray = ((IntArrayCuckoo) keyToOffset).getAsBytesWithHeader(headerSize);
    elementBuf = ByteBuffer.wrap(elementArray);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    // Skip storing LT as the legacy format is just V

    //  Store legacy format
    elementBuf.putInt(mapSize); // legacy size does *not* include header etc., just the cuckoo map
    keyToOffset.getConfig().persist(elementBuf, elementBuf.position());
    //elementBuf.position(0/*legacyPersistedSize*/);
    elementBuf.rewind();
    //System.out.printf("legacyPersistedSize %d\n", legacyPersistedSize);

    return new KeyToOffsetMapElementV0(elementBuf);
  }
}

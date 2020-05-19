package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.cloud.dht.daemon.storage.RAMOffsetListStore;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;

/**
 * Mimics LTV storage format for legacy storage.
 */
public class OffsetListsElementV0 extends OffsetListsElement {
  public OffsetListsElementV0(ByteBuffer buf) {
    super(buf);
  }
    
    /*
    public static OffsetListsElementV0 fromFSMBuffer(ByteBuffer fsmBuf, int legacyPersistedSize) {
        ByteBuffer  listsBuf;
        
        listsBuf = BufferUtil.sliceAt(fsmBuf, legacyPersistedSize);
        return new OffsetListsElementV0(listsBuf);
    }
    */

  public static OffsetListsElementV0 fromFSMBuffer(ByteBuffer fsmBuf) {
    ByteBuffer elementBuf;
    int legacyMapLength;

    legacyMapLength = fsmBuf.getInt(KeyToOffsetMapElementV0.legacyLengthOffset);
    elementBuf = BufferUtil.sliceAt(fsmBuf, legacyMapLength + KeyToOffsetMapElementV0.headerSize);
    //System.out.printf("legacy %d\n", new KeyToOffsetMapElementV0(BufferUtil.duplicate(fsmValueBuf))
    // .getLegacyPersistedSize());
    //System.out.printf("legacyMapLength %d\n", legacyMapLength);
    //System.out.printf("%s\n", elementBuf);
    //System.out.printf("%s\n", StringUtil.byteBufferToHexString(elementBuf));
    return new OffsetListsElementV0(elementBuf);
  }

  private int getLegacyPersistedSize() {
    return buf.capacity();
  }

  public int getLength() {
    return getLegacyPersistedSize() + NumConversion.BYTES_PER_INT * 2;
  }

  public int getType() {
    return FSMElementType.OffsetLists.ordinal();
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

  ////////////////////////////////////

  /*
   * Length   (4)
   * Type     (4)
   * Value:
   *  OffstListStore
   */

  public static OffsetListsElementV0 create(RAMOffsetListStore offsetListStore) {
    ByteBuffer elementBuf;
    int offsetStoreSize;
    int legacyPersistedSize;

    offsetStoreSize = offsetListStore.persistedSizeBytes();
    legacyPersistedSize = offsetStoreSize;

    elementBuf = ByteBuffer.allocate(legacyPersistedSize);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    // Skip LT as we store only V for legacy

    // Store V
    offsetListStore.persist(elementBuf);
    elementBuf.rewind();

    return new OffsetListsElementV0(elementBuf);
  }
}

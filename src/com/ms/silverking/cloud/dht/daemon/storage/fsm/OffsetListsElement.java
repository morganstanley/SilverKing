package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.daemon.storage.BufferOffsetListStore;
import com.ms.silverking.cloud.dht.daemon.storage.OffsetListStore;
import com.ms.silverking.cloud.dht.daemon.storage.RAMOffsetListStore;
import com.ms.silverking.numeric.NumConversion;

public class OffsetListsElement extends LTVElement {
  private static final boolean debugPersistence = false;

  public OffsetListsElement(ByteBuffer buf) {
    super(buf);
  }

  public OffsetListStore getOffsetListStore(NamespaceOptions nsOptions) {
    return BufferOffsetListStore.createFromBuf(getValueBuffer(), nsOptions);
  }

  ////////////////////////////////////

  /*
   * Length   (4)
   * Type     (4)
   * Value:
   *  OffstListStore
   */

  public static OffsetListsElement create(RAMOffsetListStore offsetListStore) {
    ByteBuffer elementBuf;
    int offsetStoreSize;
    int elementSize;
    int legacyPersistedSize;

    offsetStoreSize = offsetListStore.persistedSizeBytes();
    legacyPersistedSize = offsetStoreSize;
    elementSize = NumConversion.BYTES_PER_INT * 2   // length + type
        + legacyPersistedSize;

    elementBuf = ByteBuffer.allocate(elementSize);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    elementBuf.putInt(elementSize);
    elementBuf.putInt(FSMElementType.OffsetLists.ordinal());

    offsetListStore.persist(elementBuf);

    elementBuf.rewind();

    //System.out.printf("offsetListsElement %s\n", StringUtil.byteBufferToHexString(elementBuf));

    return new OffsetListsElement(elementBuf);
  }
}

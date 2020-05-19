package com.ms.silverking.cloud.dht.daemon.storage;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.collection.CuckooConfig;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.numeric.NumConversion;

public class BufferOffsetListStore implements OffsetListStore {
  private final ByteBuffer buf;
  private final boolean supportsStorageTime;
  private final int numLists; // optimization

  /*
   * As discussed in RAMOffsetListStore, indexing to
   * the lists is 1-based externally and zero-based internally.
   */

  public BufferOffsetListStore(ByteBuffer buf, NamespaceOptions nsOptions) {
    this.buf = buf;
    supportsStorageTime = nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS;
    numLists = getNumLists(buf);
  }

  public static BufferOffsetListStore createFromHTBuf(ByteBuffer rawHTBuf, NamespaceOptions nsOptions) {
    ByteBuffer buf;
    int htBufSize;

    htBufSize = rawHTBuf.getInt(0);
    if (debug) {
      System.out.println("\thtBufSize: " + htBufSize);
    }
    buf = BufferUtil.sliceAt(rawHTBuf, htBufSize + CuckooConfig.BYTES + NumConversion.BYTES_PER_INT);
    return new BufferOffsetListStore(buf, nsOptions);
  }

  public static BufferOffsetListStore createFromBuf(ByteBuffer buf, NamespaceOptions nsOptions) {
    ByteBuffer _buf;

    _buf = BufferUtil.duplicate(buf);
    return new BufferOffsetListStore(_buf, nsOptions);
  }

  private static int getNumLists(ByteBuffer buf) {
    return buf.getInt(0);
  }

  public int getNumLists() {
    return numLists;
  }

  @Override
  public OffsetList newOffsetList() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OffsetList getOffsetList(int index) {
    try {
      ByteBuffer listBuf;
      int listOffset;
      int listNumEntries;
      int listSizeBytes;

      //ensureBufInitialized(); // lazy, chance of multiple calls
      if (index <= 0) {
        throw new RuntimeException("index <= 0: " + index);
      }
      if (index > getNumLists()) {
        throw new RuntimeException("index > getNumLists(): " + index);
      }
      if (debug) {
        System.out.println("getOffsetList index: " + index);
      }
      // in the line below, the -1 to translate to an internal index
      // and the +1 of the length offset cancel out
      listOffset = buf.getInt(index * NumConversion.BYTES_PER_INT);
      if (listOffset < 0 || listOffset >= buf.limit()) {
        Log.warningf("listOffset: %d", listOffset);
        Log.warningf("buf.limit(): %d", buf.limit());
        throw new InvalidOffsetListIndexException(index);
      }
      if (debug) {
        System.out.printf("listOffset %d\t%x\n", listOffset, listOffset);
        System.out.println(buf);
      }
      listBuf = BufferUtil.sliceAt(buf, listOffset);
      listNumEntries = listBuf.getInt(OffsetListBase.listSizeOffset);
      listSizeBytes = listNumEntries * OffsetListBase.entrySizeBytes(
          supportsStorageTime) + OffsetListBase.persistedHeaderSizeBytes;
      if (debug) {
        int listStoredIndex;

        listStoredIndex = listBuf.getInt(OffsetListBase.indexOffset);
        System.out.printf("listStoredIndex %d %x\n", listStoredIndex, listStoredIndex);
        System.out.printf("index %d\tlistNumEntries %d\tlistSizeBytes %d\n", index, listNumEntries, listSizeBytes);
      }
      listBuf.limit(listSizeBytes);
      return new BufferOffsetList(listBuf, supportsStorageTime);
    } catch (RuntimeException re) { // FUTURE - consider removing debug
            /*
            int         listOffset;
            
            //listOffset = buf.getInt(index * NumConversion.BYTES_PER_INT);
            listOffset = buf.getInt(listOffset + index * NumConversion.BYTES_PER_INT);
            Log.warningf("getOffsetList index: %d", index);
            Log.warningf("%d\t%x\n", listOffset, listOffset);
            */
      Log.warningf("%s", buf);
      re.printStackTrace();
      throw re;
    }
  }

  public void displayForDebug() {
    int numLists;

    numLists = getNumLists();
    System.out.printf("numLists %d\n", numLists);
    for (int i = 0; i < numLists; i++) {
      OffsetList l;

      System.out.printf("List %d\n", i);
      l = getOffsetList(i + 1);
      l.displayForDebug();
    }
  }
}

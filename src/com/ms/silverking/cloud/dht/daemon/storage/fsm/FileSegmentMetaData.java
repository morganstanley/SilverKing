package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Set;

import com.ms.silverking.cloud.dht.collection.IntArrayCuckoo;
import com.ms.silverking.cloud.dht.daemon.storage.RAMOffsetListStore;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.log.Log;

public class FileSegmentMetaData {
  private final ByteBuffer fsmValueBuf;
  private final FSMHeader header;
  private final FileSegmentStorageFormat fsStorageFormat;

  /*
   * FileSegmentMetaData appears at the end of the file segment after
   * the data segment. While the data segment is fixed in size, the meta
   * data is variable in size.
   *
   * Originally, the meta data consisted of a map from keys to offsets in
   * the data segment followed by a list of offsets (for keys with multiple
   * values, in which case the offset in the above map would point to the
   * list offset).
   *
   * The original meta data format is referred to in the FSM code as the
   * version zero or V0 format.
   *
   * The new meta data format follows the data segment with a header,
   * followed by a number of LTV elements. Typically, the first two LTV
   * elements are the map and then the offset lists.
   *
   * LTV Format:
   *
   * V0 Format:
   */

  public enum MetaDataPrereadMode {NoPrearead, Preread}

  ;

  private static final boolean debugPersist = false;
  private static final boolean debugCreate = false;

  public FileSegmentMetaData(ByteBuffer fsmValueBuf, FSMHeader header, FileSegmentStorageFormat fsStorageFormat) {
    this.fsmValueBuf = fsmValueBuf;
    this.header = header;
    this.fsStorageFormat = fsStorageFormat;
  }

  /**
   * Called by read()
   *
   * @param fsmBuf
   * @return
   */
  public static FileSegmentMetaData create(ByteBuffer fsmBuf, FileSegmentStorageFormat fsStorageFormat) {
    FSMHeader header;
    ByteBuffer fsmValueBuf;

    if (debugCreate) {
      Log.warningf("fsmBuf %s", fsmBuf);
    }
    if (fsStorageFormat.containsFSMHeader()) {
      FSMHeaderElement headerElement;

      //System.out.printf("%s\n", StringUtil.byteBufferToHexString(fsmBuf));
      headerElement = new FSMHeaderElement(fsmBuf);
      // First parse the header; the header will have an entry
      // for each element in the FSM
      header = headerElement.toFSMHeader();
      fsmValueBuf = BufferUtil.sliceAt(fsmBuf, headerElement.getLength());
    } else {
      header = null;
      fsmValueBuf = fsmBuf;
    }
    // Now return a FileSegmentMetaData element
    // FSM.getElement() can be used to read the elements themselves
    return new FileSegmentMetaData(fsmValueBuf, header, fsStorageFormat);
  }

  public LTVElement getElement(FSMElementType type) {
    ByteBuffer elementBuf;

    //System.out.printf("getElement %s\n", type);
    if (fsStorageFormat.dataSegmentIsLTV()) {
      int offset;

      offset = header.getElementOffset(type);
      if (offset < 0) {
        //System.out.printf("Unable to find element: %s\n", type);
        return null;
      }
      elementBuf = LTVElement.readElementBuffer(fsmValueBuf, offset);
      switch (type) {
      case OffsetMap:
        return new KeyToOffsetMapElement(elementBuf);
      case OffsetLists:
        return new OffsetListsElement(elementBuf);
      case InvalidatedOffsets:
        return new InvalidatedOffsetsElement(elementBuf);
      default:
        return null;
      }
    } else {
      switch (type) {
      case OffsetMap:
        elementBuf = fsmValueBuf;
        return KeyToOffsetMapElementV0.fromFSMBuffer(elementBuf);
      case OffsetLists:
        return OffsetListsElementV0.fromFSMBuffer(fsmValueBuf);
      default:
        return null;
      }
    }
  }

  public static FileSegmentMetaData read(FileSegmentStorageFormat fsStorageFormat, RandomAccessFile raFile,
      long dataSegmentSize, MetaDataPrereadMode prereadMode) throws IOException {
    ByteBuffer fsmBuf;

    fsmBuf = raFile.getChannel().map(MapMode.READ_ONLY, dataSegmentSize, raFile.length() - dataSegmentSize);
    if (prereadMode == MetaDataPrereadMode.Preread) {
      ((MappedByteBuffer) fsmBuf).load();
    }
    fsmBuf = fsmBuf.order(ByteOrder.nativeOrder());
    return create(fsmBuf, fsStorageFormat);
  }

  ///////////////////////////////////////

  public static void persist(FileSegmentStorageFormat fsStorageFormat, RandomAccessFile raFile, int dataSegmentSize,
      IntArrayCuckoo keyToOffset, RAMOffsetListStore offsetListStore, Set<Integer> invalidatedOffsets)
      throws IOException {
    FSMHeaderElement fsmHeaderElement;
    KeyToOffsetMapElement keyToOffsetMapElement;
    OffsetListsElement offsetListsElement;
    InvalidatedOffsetsElement invalidatedOffsetsElement;
    FSMHeader fsmHeader;
    ByteBuffer fsmBuf;
    int fsmLength;

    if (fsStorageFormat.dataSegmentIsLTV()) {
      keyToOffsetMapElement = KeyToOffsetMapElement.create(keyToOffset);
      offsetListsElement = OffsetListsElement.create(offsetListStore);
      invalidatedOffsetsElement = InvalidatedOffsetsElement.create(invalidatedOffsets);
    } else {
      keyToOffsetMapElement = KeyToOffsetMapElementV0.create(keyToOffset);
      offsetListsElement = OffsetListsElementV0.create(offsetListStore);
      invalidatedOffsetsElement = null;
    }

    if (fsStorageFormat.containsFSMHeader()) {
      fsmHeader = FSMHeader.create(keyToOffsetMapElement, offsetListsElement, invalidatedOffsetsElement);
      fsmHeaderElement = FSMHeaderElement.createFromHeader(fsmHeader);
      fsmLength =
          fsmHeaderElement.getLength() + keyToOffsetMapElement.getLength() + offsetListsElement.getLength() + invalidatedOffsetsElement.getLength();
    } else {
      fsmHeader = null;
      fsmHeaderElement = null;
      fsmLength =
          keyToOffsetMapElement.getValueLength() + KeyToOffsetMapElementV0.headerSize + offsetListsElement.getValueLength();
    }
    fsmBuf = raFile.getChannel().map(MapMode.READ_WRITE, dataSegmentSize, fsmLength).order(ByteOrder.nativeOrder());
    if (fsStorageFormat.containsFSMHeader()) {
      fsmBuf.put(fsmHeaderElement.getBuffer());
      //System.out.printf("header  %s\n", StringUtil.byteBufferToHexString(fsmHeaderElement.getBuffer()));
    }
    if (fsStorageFormat.dataSegmentIsLTV()) {
      //System.out.printf("map     %s\n", StringUtil.byteBufferToHexString(keyToOffsetMapElement.getBuffer()));
      //System.out.printf("lists   %s\n", StringUtil.byteBufferToHexString(offsetListsElement.getBuffer()));
      //System.out.printf("invalid %s\n", StringUtil.byteBufferToHexString(invalidatedOffsetsElement.getBuffer()));
      fsmBuf.put(keyToOffsetMapElement.getBuffer());
      fsmBuf.put(offsetListsElement.getBuffer());
      fsmBuf.put(invalidatedOffsetsElement.getBuffer());
    } else {
      fsmBuf.put(keyToOffsetMapElement.getValueBuffer());
      //System.out.printf("%s\n", fsmBuf);
      //System.out.printf("%s\n", offsetListsElement.getValueBuffer());
      fsmBuf.put(offsetListsElement.getValueBuffer());
    }
    ((MappedByteBuffer) fsmBuf).force();

    // The below two methods should be extraneous. They are
    // left here commented out as a reminder of additional methods available
    // should MappedByteBuffer.force() prove to not fulfill its contract.
    //raFile.getChannel().force(true);
    //raFile.getFD().sync();

    raFile.close();
    raFile = null;
  }
}

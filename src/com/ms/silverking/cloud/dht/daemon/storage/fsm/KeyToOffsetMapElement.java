package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.ms.silverking.cloud.dht.collection.CuckooBase;
import com.ms.silverking.cloud.dht.collection.CuckooConfig;
import com.ms.silverking.cloud.dht.collection.IntArrayCuckoo;
import com.ms.silverking.cloud.dht.collection.IntBufferCuckoo;
import com.ms.silverking.cloud.dht.collection.WritableCuckooConfig;
import com.ms.silverking.io.util.BufferUtil;
import com.ms.silverking.numeric.NumConversion;

public class KeyToOffsetMapElement extends LTVElement {
  private static final boolean debugPersistence = false;

  public KeyToOffsetMapElement(ByteBuffer buf) {
    super(buf);
  }

  public CuckooBase getKeyToOffsetMap() {
        /*
        if (segmentIndexLocation == SegmentIndexLocation.RAM) {
            byte[]      _htBufArray;
            
            _htBufArray = new byte[(int)(raFile.length() - dataSegmentSize)];
            raFile.seek(dataSegmentSize);
            raFile.read(_htBufArray);
            rawHTBuf = ByteBuffer.wrap(_htBufArray);
        } else {
            rawHTBuf = raFile.getChannel().map(MapMode.READ_ONLY, dataSegmentSize, raFile.length() - dataSegmentSize);
            if (segmentPrereadMode == SegmentPrereadMode.Preread) {
                ((MappedByteBuffer)rawHTBuf).load();
            }
        }
        rawHTBuf = rawHTBuf.order(ByteOrder.nativeOrder());
        try {
        htBuf = ((ByteBuffer)rawHTBuf.duplicate().position(NumConversion.BYTES_PER_INT + CuckooConfig.BYTES)).slice();
        } catch (RuntimeException re) {
            System.out.println(nsDir);
            System.out.println(segmentNumber);
            System.out.println(dataMapMode);
            System.out.println();
            System.out.println(dataBuf);
            System.out.printf("%d\t%d\t%d\t%d\n", dataSegmentSize, raFile.length(), 
                              dataSegmentSize, raFile.length() - dataSegmentSize);
            System.out.println();
            System.out.println(rawHTBuf);
            throw re;
        }
        */

    ByteBuffer rawHTBuf;
    ByteBuffer htBuf;
    WritableCuckooConfig segmentCuckooConfig;
    CuckooBase keyToOffset;
    int htBufSize;
    int htTotalEntries;

    rawHTBuf = getValueBuffer();

    // FUTURE - cache the below number or does the segment cache do this well enough? (also cache the
    // segmentCuckooConfig...)
    htBufSize = rawHTBuf.getInt(0);
    try {
      htBuf = BufferUtil.sliceAt(rawHTBuf, NumConversion.BYTES_PER_INT + CuckooConfig.BYTES);
    } catch (RuntimeException re) {
            /*
            System.out.println(nsDir);
            System.out.println(segmentNumber);
            System.out.println(dataMapMode);
            System.out.println();
            System.out.println(dataBuf);
            System.out.printf("%d\t%d\t%d\t%d\n", dataSegmentSize, raFile.length(), 
                              dataSegmentSize, raFile.length() - dataSegmentSize);
            System.out.println();
            System.out.println(rawHTBuf);
            */
      throw re;
    }

    rawHTBuf = rawHTBuf.order(ByteOrder.nativeOrder());

    htTotalEntries = htBufSize / (NumConversion.BYTES_PER_LONG * 2 + NumConversion.BYTES_PER_INT);
    segmentCuckooConfig = new WritableCuckooConfig(CuckooConfig.read(rawHTBuf, NumConversion.BYTES_PER_INT),
        -1); // FIXME - verify -1
    segmentCuckooConfig = segmentCuckooConfig.newTotalEntries(htTotalEntries);

    //System.out.printf("%s\n", segmentCuckooConfig);
    //System.out.printf("%s\n", htBuf);
    //System.out.printf("%s\n", StringUtil.byteBufferToHexString(htBuf));

    keyToOffset = new IntBufferCuckoo(segmentCuckooConfig, htBuf);
    return keyToOffset;
/*
        return new FileSegment(nsDir, segmentNumber, accessMode, raFile, 
                               dataBuf, htBuf, invalidatedOffsetsBuf, 
                               segmentCuckooConfig.newTotalEntries(htTotalEntries), 
                               new BufferOffsetListStore(rawHTBuf, nsOptions), dataSegmentSize);
                               */
  }

  ////////////////////////////////////

  /*
   * Length   (4)
   * Type     (4)
   * Value:
   * HTLength (4)
   * CuckooConfig (CuckooConfig.BYTES)
   */

  public static KeyToOffsetMapElement create(CuckooBase keyToOffset) {
    ByteBuffer elementBuf;
    byte[] elementArray;
    int mapSize;
    int headerSize;
    int elementSize;
    int legacyPersistedSize;

    mapSize = ((IntArrayCuckoo) keyToOffset).persistedSizeBytes();

    // FIXME
    legacyPersistedSize = NumConversion.BYTES_PER_INT + CuckooConfig.BYTES + mapSize;
    elementSize = NumConversion.BYTES_PER_INT * 2   // length + type
        + legacyPersistedSize;
    headerSize = elementSize - mapSize;

    elementArray = ((IntArrayCuckoo) keyToOffset).getAsBytesWithHeader(headerSize);
    elementBuf = ByteBuffer.wrap(elementArray);
    elementBuf = elementBuf.order(ByteOrder.nativeOrder());

    elementBuf.putInt(elementSize);
    elementBuf.putInt(FSMElementType.OffsetMap.ordinal());

    elementBuf.putInt(mapSize);
    keyToOffset.getConfig().persist(elementBuf, elementBuf.position());
    elementBuf.position(elementSize);

    elementBuf.rewind();

    return new KeyToOffsetMapElement(elementBuf);
  }
}

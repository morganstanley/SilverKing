package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.Set;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.collection.CuckooBase;
import com.ms.silverking.cloud.dht.collection.IntArrayCuckoo;
import com.ms.silverking.cloud.dht.common.SegmentIndexLocation;
import com.ms.silverking.cloud.dht.daemon.storage.fsm.FSMElementType;
import com.ms.silverking.cloud.dht.daemon.storage.fsm.FileSegmentMetaData;
import com.ms.silverking.cloud.dht.daemon.storage.fsm.FileSegmentStorageFormat;
import com.ms.silverking.cloud.dht.daemon.storage.fsm.InvalidatedOffsetsElement;
import com.ms.silverking.cloud.dht.daemon.storage.fsm.KeyToOffsetMapElement;
import com.ms.silverking.cloud.dht.daemon.storage.fsm.OffsetListsElement;
import com.ms.silverking.log.Log;

public class FileSegment extends WritableSegmentBase {
  private final AccessMode accessMode;
  private RandomAccessFile raFile;
  private final FileSegmentStorageFormat storageFormat;

  private static final String roFileMode = "r";
  private static final String rwFileMode = "rw";
  private static final String rwdFileMode = "rwd";

  enum AccessMode {
    Creation,     // Creation of segment. Only allowed once per segment
    Repair,       // Repair a segment with a damaged index; recovery only
    Update,       // Update segment entries, but not index
    ReadOnly,     // Read-only access to segment
    ReadIndexOnly // Read-only access to the segment index only; data proper is not mapped
  }

  ;

  enum SyncMode {NoSync, Sync}

  ;

  enum SegmentPrereadMode {NoPreread, Preread}

  ;

  private static final boolean debugMetaData = false;

  private static String syncModeToFileOpenMode(SyncMode syncMode) {
    return syncMode == SyncMode.NoSync ? rwFileMode : rwdFileMode;
  }

  static final FileSegment create(File nsDir, int segmentNumber, int dataSegmentSize, SyncMode syncMode,
      NamespaceOptions nsOptions) throws IOException {
    RandomAccessFile raFile;
    byte[] header;
    ByteBuffer dataBuf;
    int indexOffset;

    indexOffset = dataSegmentSize;
    raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), syncModeToFileOpenMode(syncMode));
    header = SegmentFormat.newHeader(segmentNumber, dataOffset, indexOffset);

    dataBuf = raFile.getChannel().map(MapMode.READ_WRITE, 0, dataSegmentSize);
    dataBuf.put(header);
    return new FileSegment(nsDir, segmentNumber, AccessMode.Creation, raFile, dataBuf, dataSegmentSize, nsOptions);
  }

  public static FileSegment openForDataUpdate(File nsDir, int segmentNumber, int dataSegmentSize, SyncMode syncMode,
      NamespaceOptions nsOptions, SegmentIndexLocation segmentIndexLocation, SegmentPrereadMode segmentPrereadMode)
      throws IOException {
    return open(nsDir, segmentNumber, AccessMode.Update, dataSegmentSize, syncMode, nsOptions, segmentIndexLocation,
        segmentPrereadMode);
  }

  public static FileSegment openReadIndexOnly(File nsDir, int segmentNumber, int dataSegmentSize,
      NamespaceOptions nsOptions) throws IOException {
    return open(nsDir, segmentNumber, AccessMode.ReadIndexOnly, dataSegmentSize, SyncMode.NoSync, nsOptions,
        SegmentIndexLocation.RAM, SegmentPrereadMode.NoPreread);
  }

  public static FileSegment openReadOnly(File nsDir, int segmentNumber, int dataSegmentSize, NamespaceOptions nsOptions)
      throws IOException {
    return openReadOnly(nsDir, segmentNumber, dataSegmentSize, nsOptions, SegmentIndexLocation.RAM,
        SegmentPrereadMode.Preread);
  }

  public static FileSegment openReadOnly(File nsDir, int segmentNumber, int dataSegmentSize, NamespaceOptions nsOptions,
      SegmentIndexLocation segmentIndexLocation, SegmentPrereadMode segmentPrereadMode) throws IOException {
    return open(nsDir, segmentNumber, AccessMode.ReadOnly, dataSegmentSize, SyncMode.NoSync, nsOptions,
        segmentIndexLocation, segmentPrereadMode);
  }

  private static FileSegment open(File nsDir, int segmentNumber, AccessMode accessMode, int dataSegmentSize,
      SyncMode syncMode, NamespaceOptions nsOptions, SegmentIndexLocation segmentIndexLocation,
      SegmentPrereadMode segmentPrereadMode) throws IOException {
    RandomAccessFile raFile;
    ByteBuffer dataBuf;
    ByteBuffer rawHTBuf;
    String fileOpenMode;
    MapMode dataMapMode;
    FileSegmentMetaData fsm;
    InvalidatedOffsetsElement ioe;
    KeyToOffsetMapElement ktome;
    OffsetListsElement ole;

    //Log.warningf("open %s %d", nsDir.toString(), segmentNumber);

    if (accessMode == AccessMode.ReadIndexOnly && nsOptions.getVersionMode() == NamespaceVersionMode.SINGLE_VERSION) {
      accessMode = AccessMode.ReadOnly;
      // Presently, the index does not contain sufficient information to allow this in SINGLE_VERSION mode
    }

    switch (accessMode) {
    case ReadOnly:      // fall through
    case ReadIndexOnly:
      dataMapMode = MapMode.READ_ONLY;
      fileOpenMode = roFileMode;
      break;
    case Update:
      dataMapMode = MapMode.READ_WRITE;
      fileOpenMode = syncModeToFileOpenMode(syncMode);
      break;
    default:
      throw new RuntimeException("Unexpected mode: " + accessMode);
    }
    raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), fileOpenMode);
    if (false) {
      // This would allow us to force each segment onto the heap
      // FUTURE: consider whether we want to keep this option
      byte[] _bufArray;

      _bufArray = new byte[dataSegmentSize];
      raFile.read(_bufArray);
      dataBuf = ByteBuffer.wrap(_bufArray);
    } else {
      if (accessMode != AccessMode.ReadIndexOnly) {
        dataBuf = raFile.getChannel().map(dataMapMode, 0, dataSegmentSize);
        // Presently, the data segment is in network byte order - big endian
        // While nice-to-have in principle for network data, the fact
        // that all usage will likely be little endian makes this a poor format
        // For now, we leave this format.
        // FUTURE - switch the data buffer to little endian
        //if (_fsStorageFormat.dataSegmentIsLTV()) {
        //    dataBuf = dataBuf.order(ByteOrder.nativeOrder());
        //}
        if (segmentPrereadMode == SegmentPrereadMode.Preread) {
          ((MappedByteBuffer) dataBuf).load();
        }
      } else {
        dataBuf = null;
      }
    }
    if (debugMetaData) {
      Log.warningf("dataSegmentSize %d", dataSegmentSize);
      Log.warningf("raFile.length() %d", raFile.length());
      Log.warningf("raFile.length() - dataSegmentSize %d", raFile.length() - dataSegmentSize);
    }
    if (segmentIndexLocation == SegmentIndexLocation.RAM) {
      byte[] _htBufArray;

      _htBufArray = new byte[(int) (raFile.length() - dataSegmentSize)];
      raFile.seek(dataSegmentSize);
      raFile.read(_htBufArray);
      rawHTBuf = ByteBuffer.wrap(_htBufArray);
    } else {
      rawHTBuf = raFile.getChannel().map(MapMode.READ_ONLY, dataSegmentSize, raFile.length() - dataSegmentSize);
      if (segmentPrereadMode == SegmentPrereadMode.Preread) {
        ((MappedByteBuffer) rawHTBuf).load();
      }
    }
    rawHTBuf = rawHTBuf.order(ByteOrder.nativeOrder());

    if (debugMetaData) {
      Log.warningf("rawHTBuf %s", rawHTBuf);
    }
    fsm = FileSegmentMetaData.create(rawHTBuf, FileSegmentStorageFormat.parse(nsOptions.getStorageFormat()));
    ktome = (KeyToOffsetMapElement) fsm.getElement(FSMElementType.OffsetMap);
    //((IntBufferCuckoo)ktome.getKeyToOffsetMap()).display();
    ole = (OffsetListsElement) fsm.getElement(FSMElementType.OffsetLists);
    //((BufferOffsetListStore)ole.getOffsetListStore(nsOptions)).displayForDebug();
    ioe = (InvalidatedOffsetsElement) fsm.getElement(FSMElementType.InvalidatedOffsets);
    return new FileSegment(nsDir, segmentNumber, accessMode, raFile, dataBuf, ktome.getKeyToOffsetMap(),
        ioe != null ? ioe.getInvalidatedOffsets() : null, ole.getOffsetListStore(nsOptions), dataSegmentSize,
        nsOptions);
  }

  static File fileForSegment(File nsDir, int segmentNumber) {
    return new File(nsDir, Integer.toString(segmentNumber));
  }

  public static ByteBuffer getDataSegment(File nsDir, int segmentNumber, int dataSegmentSize) throws IOException {
    RandomAccessFile raFile;
    ByteBuffer dataBuf;

    raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), roFileMode);
    dataBuf = raFile.getChannel().map(MapMode.READ_ONLY, 0, dataSegmentSize);
    raFile.close();
    return dataBuf;
  }

  static final FileSegment openForRecovery(File nsDir, int segmentNumber, int dataSegmentSize, SyncMode syncMode,
      NamespaceOptions nsOptions) throws IOException {
    RandomAccessFile raFile;
    ByteBuffer dataBuf;
    FileSegment segment;

    raFile = new RandomAccessFile(fileForSegment(nsDir, segmentNumber), syncModeToFileOpenMode(syncMode));

    dataBuf = raFile.getChannel().map(MapMode.READ_WRITE, 0, dataSegmentSize);
    segment = new FileSegment(nsDir, segmentNumber, AccessMode.Repair, raFile, dataBuf, dataSegmentSize, nsOptions);
    return segment;
  }

  // Open read-only or for update
  private FileSegment(File nsDir, int segmentNumber, AccessMode accessMode, RandomAccessFile raFile, ByteBuffer dataBuf,
      CuckooBase keyToOffset, Set<Integer> invalidatedOffsets, OffsetListStore offsetListStore, int dataSegmentSize,
      NamespaceOptions nsOptions) throws IOException {
    super(nsDir, segmentNumber, dataBuf, keyToOffset, invalidatedOffsets, offsetListStore, dataSegmentSize);
    this.accessMode = accessMode;
    if (accessMode != AccessMode.ReadOnly && accessMode != AccessMode.Update && accessMode != AccessMode.ReadIndexOnly) {
      throw new RuntimeException("Unexpected mode: " + accessMode);
    }
    raFile.close();
    this.raFile = null;
    this.storageFormat = FileSegmentStorageFormat.parse(nsOptions.getStorageFormat());
  }

  // called from Create
  private FileSegment(File nsDir, int segmentNumber, AccessMode accessMode, RandomAccessFile raFile, ByteBuffer dataBuf,
      int dataSegmentSize, NamespaceOptions nsOptions) throws IOException {
    super(nsDir, segmentNumber, dataBuf, StoreConfiguration.fileInitialCuckooConfig, dataSegmentSize, nsOptions);
    this.accessMode = accessMode;
    this.raFile = raFile;
    this.storageFormat = FileSegmentStorageFormat.parse(nsOptions.getStorageFormat());
  }

  public void persist() throws IOException {
    ((MappedByteBuffer) dataBuf).force();
    FileSegmentMetaData.persist(storageFormat, raFile, dataSegmentSize, (IntArrayCuckoo) keyToOffset,
        (RAMOffsetListStore) offsetListStore, invalidatedOffsets);
    raFile.close();
    raFile = null;
    close();
  }

  public void close() {
    dataBuf = null;
  }

  public void displayForDebug() {
    int i;

    i = 1;
    while (true) {
      OffsetList offsetList;

      try {
        offsetList = offsetListStore.getOffsetList(i);
      } catch (RuntimeException e) {
        offsetList = null;
      }
      if (offsetList != null) {
        Log.warningf("Offset list: %s", i);
        offsetList.displayForDebug();
        i++;
      } else {
        break;
      }
    }
  }
}

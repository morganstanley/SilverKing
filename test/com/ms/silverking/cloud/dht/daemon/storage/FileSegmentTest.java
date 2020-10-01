package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.io.Files;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.SegmentIndexLocation;
import com.ms.silverking.cloud.dht.common.SimpleKey;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.daemon.storage.fsm.FileSegmentStorageFormat;
import com.ms.silverking.text.StringUtil;
import org.junit.Test;

public class FileSegmentTest {
  private static final NamespaceOptions nsOptions;
  private static final File nsDir;

  private enum SegmentTest {Read, Write}

  ;

  static {
    File tempDir;

    tempDir = Files.createTempDir();
    nsOptions = DHTConstants.defaultNamespaceOptions.versionMode(NamespaceVersionMode.SYSTEM_TIME_NANOS);
    nsDir = new File(tempDir, "/ns.test");
    nsDir.mkdirs();
    System.out.printf("nsDir: %s\n", nsDir);
  }

  private InternalRetrievalOptions getRetrievalOptions() {
    return new InternalRetrievalOptions(nsOptions.getDefaultGetOptions());
  }

  private StorageParameters getStorageParameters(byte[] value) {
    long version;
    int uncompressedSize;
    int compressedSize;
    short lockSeconds;
    short ccss;
    byte[] checksum;
    byte[] valueCreator;
    long creationTime;

    version = 1;
    uncompressedSize = value.length;
    compressedSize = value.length;
    lockSeconds = 0;
    ccss = 0;
    checksum = new byte[0];
    valueCreator = SimpleValueCreator.forLocalProcess().getBytes();
    creationTime = System.currentTimeMillis();
    return new StorageParameters(version, uncompressedSize, compressedSize, lockSeconds, ccss, checksum, valueCreator,
        creationTime);
  }

  @Test
  public void test() {
    int segmentNumber;
    int dataSegmentSize;
    int numKeys;
    int valuesPerKey;

    segmentNumber = 0;
    dataSegmentSize = 2048;
    numKeys = 2;
    valuesPerKey = 1;
    _test(segmentNumber, "0", EnumSet.of(SegmentTest.Write), numKeys, dataSegmentSize, valuesPerKey,
        new FileSegmentStorageFormat(0));
    _test(segmentNumber, "0", EnumSet.of(SegmentTest.Write), numKeys, dataSegmentSize, valuesPerKey,
        new FileSegmentStorageFormat(2));
  }

  @Test
  public void test1() {
    int segmentNumber;
    int dataSegmentSize;
    int numKeys;
    int valuesPerKey;

    segmentNumber = 1;
    dataSegmentSize = 32768;
    numKeys = 2;
    valuesPerKey = 1;
    _test(segmentNumber, "1", EnumSet.of(SegmentTest.Write, SegmentTest.Read), numKeys, dataSegmentSize, valuesPerKey,
        new FileSegmentStorageFormat(0));
    _test(segmentNumber, "1", EnumSet.of(SegmentTest.Write, SegmentTest.Read), numKeys, dataSegmentSize, valuesPerKey,
        new FileSegmentStorageFormat(2));
  }

  @Test
  public void test2() {
    int segmentNumber;
    int dataSegmentSize;
    int numKeys;
    int valuesPerKey;

    segmentNumber = 2;
    dataSegmentSize = 32768;
    numKeys = 2;
    valuesPerKey = 4;
    _test(segmentNumber, "2", EnumSet.of(SegmentTest.Write, SegmentTest.Read), numKeys, dataSegmentSize, valuesPerKey,
        new FileSegmentStorageFormat(0));
    _test(segmentNumber, "2", EnumSet.of(SegmentTest.Write, SegmentTest.Read), numKeys, dataSegmentSize, valuesPerKey,
        new FileSegmentStorageFormat(2));
  }

  private void _test(int segmentNumber, String id, Set<SegmentTest> tests, int numKeys, int dataSegmentSize,
      int valuesPerKey, FileSegmentStorageFormat storageFormat) {
    try {
      FileSegment fs;
      DHTKey[] keys;
      String[] values;
      NamespaceOptions _nsOptions;

      _nsOptions = nsOptions.storageFormat(storageFormat.toString());
      System.out.printf("\n\ntest %s\n", id);
      fs = FileSegment.create(nsDir, segmentNumber, dataSegmentSize, FileSegment.SyncMode.NoSync, _nsOptions);

      keys = new DHTKey[numKeys];
      for (int i = 0; i < numKeys; i++) {
        keys[i] = new SimpleKey(i, i);
      }

      if (tests.contains(SegmentTest.Write)) {
        for (int i = 0; i < numKeys; i++) {
          DHTKey key;
          StorageParameters storageParams;

          key = keys[i];
          for (int j = 0; j < valuesPerKey; j++) {
            byte[] value;
            byte[] userData;

            value = String.format("[VALUE %d.%d]", i, j).getBytes();
            storageParams = getStorageParameters(value);
            userData = new byte[0];
            System.out.printf("%s -> %s\n", key, new String(value));
            fs.put(key, ByteBuffer.wrap(value), storageParams, userData, _nsOptions);
          }
        }
        fs.persist();
        fs.close();
      }

      if (tests.contains(SegmentTest.Read)) {
        fs = FileSegment.openForDataUpdate(nsDir, segmentNumber, dataSegmentSize, FileSegment.SyncMode.NoSync,
            _nsOptions, SegmentIndexLocation.RAM, FileSegment.SegmentPrereadMode.Preread);
        for (DHTKey key : keys) {
          InternalRetrievalOptions iro;
          ByteBuffer result;

          iro = getRetrievalOptions();
          result = fs.retrieve(key, iro);
          System.out.printf("%s => %s\n", KeyUtil.keyToString(key), StringUtil.byteBufferToString(result));
        }
        fs.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}

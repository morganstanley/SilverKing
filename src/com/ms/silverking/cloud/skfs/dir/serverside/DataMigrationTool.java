package com.ms.silverking.cloud.skfs.dir.serverside;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceServerSideCode;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.common.CCSSUtil;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.DataSegmentWalkEntry;
import com.ms.silverking.cloud.dht.daemon.storage.DataSegmentWalker;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment;
import com.ms.silverking.cloud.dht.daemon.storage.NamespacePropertiesIO;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.serverside.SSStorageParameters;
import com.ms.silverking.cloud.skfs.dir.DirectoryInPlace;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.io.FileUtil;
import com.ms.silverking.io.util.BufferUtil;

public class DataMigrationTool {
  private static final boolean displayDirs = false;

  private final Map<String, File> latestVersions;
  private static int numEntriesFiltered;
  private static int numEntriesIncluded;

  public DataMigrationTool() {
    latestVersions = new HashMap<>();
  }

  public void walk(File sourceDir, File destDir, File savedSegmentsDir, Set<DHTKey> filteredPaths) throws IOException {
    List<Long> segments;

    segments = FileUtil.numericFilesInDirAsSortedLongList(sourceDir);
    for (long segment : segments) {
      walk(sourceDir, destDir, (int) segment, filteredPaths);
    }

    if (sourceDir.equals(destDir)) {
      saveOldSegments(sourceDir, savedSegmentsDir, segments);

      modifyProperties(sourceDir);
    } else {
      Files.copy(new File(sourceDir, "properties").toPath(), new File(destDir, "properties").toPath());
      modifyProperties(destDir);
    }
  }

  private void saveOldSegments(File sourceDir, File savedSegmentsDir, List<Long> segments) {
    for (long segment : segments) {
      File segmentFile;
      File savedSegmentFile;

      segmentFile = new File(sourceDir, Long.toString(segment));
      savedSegmentFile = new File(savedSegmentsDir, Long.toString(segment));
      if (!segmentFile.renameTo(savedSegmentFile)) {
        throw new RuntimeException(
            "Failed to rename " + segmentFile.getAbsolutePath() + " to " + savedSegmentFile.getAbsolutePath());
      }
    }
  }

  private void modifyProperties(File sourceDir) throws IOException {
    NamespaceProperties originalProperties;
    NamespaceOptions originalOptions;
    NamespaceProperties modifiedProperties;
    NamespaceOptions modifiedOptions;

    originalProperties = NamespacePropertiesIO.read(sourceDir);
    originalOptions = originalProperties.getOptions();

    modifiedOptions = originalOptions.namespaceServerSideCode(
        new NamespaceServerSideCode("", "com.ms.silverking.cloud.skfs.dir.serverside.DirectoryServer",
            "com.ms.silverking.cloud.skfs.dir.serverside.DirectoryServer"));
    modifiedProperties = originalProperties.options(modifiedOptions);

    NamespacePropertiesIO.rewrite(sourceDir, modifiedProperties);
  }

  public void walk(File nsDir, File ssDir, int segmentNumber, Set<DHTKey> filteredPaths) throws IOException {
    ByteBuffer dataBuf;
    DataSegmentWalker dsWalker;
    NamespaceProperties nsProperties;
    NamespaceOptions nsOptions;

    nsProperties = NamespacePropertiesIO.read(nsDir);
    nsOptions = nsProperties.getOptions();
    dataBuf = FileSegment.getDataSegment(nsDir, segmentNumber, nsOptions.getSegmentSize());
    dsWalker = new DataSegmentWalker(dataBuf);
    while (dsWalker.hasNext()) {
      DataSegmentWalkEntry entry;

      entry = dsWalker.next();
      System.out.println(entry.getOffset() + " " + entry + "\t" + new Date(
          SystemTimeUtil.systemTimeNanosToEpochMillis(entry.getCreationTime())));
      migrateEntry(entry, ssDir, filteredPaths);
    }
  }

  private void migrateEntry(DataSegmentWalkEntry entry, File ssDir, Set<DHTKey> filteredPaths) throws IOException {
    ByteBuffer valueBuf;
    byte[] value;
    DHTKey key;

    //System.out.printf("\n ------------- \n%s\n", StringUtil.byteBufferToHexString(entry.getStoredFormat()));
    valueBuf = entry.getValue();
    value = BufferUtil.arrayCopy(valueBuf);
    //System.out.printf("\n......... \n%s\n", StringUtil.byteArrayToHexString(value));

    key = entry.getKey();
    if (!filteredPaths.contains(key)) {
      File dirDir;
      StorageParameters newSP;
      StorageParameters o;
      File destFile;
      String keyString;
      File prev;

      numEntriesIncluded++;
      keyString = KeyUtil.keyToString(entry.getKey());
      dirDir = new File(ssDir, keyString);
      if (!dirDir.exists()) {
        if (!dirDir.mkdir()) {
          throw new RuntimeException("Unable to create: " + dirDir);
        }
      }
      o = entry.getStorageParameters();
      newSP = new StorageParameters(o.getVersion(), value.length, value.length, o.getLockSeconds(),
          CCSSUtil.createCCSS(Compression.NONE, o.getChecksumType(), o.getStorageState()), o.getChecksum(),
          o.getValueCreator(), o.getCreationTime());
      destFile = new File(dirDir, Long.toString(entry.getVersion()));
      FileUtil.writeToFile(destFile, StorageParameterSerializer.serialize(newSP), value);

      prev = latestVersions.get(keyString);
      if (prev != null) {
        prev.delete();
      }
      latestVersions.put(keyString, destFile);

      if (displayDirs) {
        EagerDirectoryInMemorySS dim;
        Pair<SSStorageParameters, byte[]> p;
        DirectoryInPlace dip;

        dim = new EagerDirectoryInMemorySS(entry.getKey(), null, entry.getStorageParameters(),
            new File(ssDir, KeyUtil.keyToString(entry.getKey())), null, false);
        p = dim.readFromDisk(entry.getStorageParameters().getVersion());
        System.out.println(
            p.getV1() + "\t" + new Date(SystemTimeUtil.systemTimeNanosToEpochMillis(p.getV1().getCreationTime())));
        //System.out.println(StringUtil.byteArrayToHexString(p.getV2()));
        dip = new DirectoryInPlace(p.getV2(), 0, p.getV2().length);
        dip.display();
      }
    } else {
      numEntriesFiltered++;
    }
  }

  public static void main(String[] args) {
    // This tool runs stand-alone with dependency of "properties" file, which only used by NSP mode for now
    if (DHTConfiguration.defaultNamespaceOptionsMode != NamespaceOptionsMode.MetaNamespace) {
      throw new IllegalArgumentException(
          "You're in the default mode of [" + DHTConfiguration.defaultNamespaceOptionsMode + "], which is not " +
              "supported by this tool");
    }

    if (args.length < 1 || args.length > 3) {
      System.out.println("args: <data dir> [destDirBase] [dirFile]");
    } else {
      try {
        File sourceDir;
        File destDirBase;
        File destDir;
        File savedSegmentsDir;
        Set<DHTKey> filteredNamespaces;

        sourceDir = new File(args[0]);
        if (!sourceDir.exists()) {
          throw new RuntimeException("sourceDir doesn't exist: " + sourceDir.getAbsolutePath());
        }
        if (args.length > 1) {
          destDirBase = new File(args[1]);
        } else {
          destDirBase = sourceDir;
        }
        destDir = new File(destDirBase, DHTConstants.ssSubDirName);
        if (!destDir.exists()) {
          if (!destDir.mkdir()) {
            throw new RuntimeException("Couldn't create destDir: " + destDir.getAbsolutePath());
          }
        } else {
          if (destDir.list().length > 0) {
            throw new RuntimeException("destDir not empty: " + destDir.getAbsolutePath());
          }
        }
        savedSegmentsDir = new File(sourceDir, "segments.saved");
        if (!savedSegmentsDir.exists()) {
          if (!savedSegmentsDir.mkdir()) {
            throw new RuntimeException("Couldn't create savedSegmentsDir: " + savedSegmentsDir.getAbsolutePath());
          }
        } else {
          if (savedSegmentsDir.list().length > 0) {
            throw new RuntimeException("savedSegmentsDir not empty: " + savedSegmentsDir.getAbsolutePath());
          }
        }
        if (args.length == 3) {
          System.out.printf("Reading filtered namespaces\n");
          filteredNamespaces = DirFilterCreator.getFilteredDirectories(args[2]);
        } else {
          System.out.printf("No filtered namespaces\n");
          filteredNamespaces = ImmutableSet.of();
        }
        new DataMigrationTool().walk(sourceDir, destDir, savedSegmentsDir, filteredNamespaces);
        System.out.printf("numEntriesIncluded: %d\n", numEntriesIncluded);
        System.out.printf("numEntriesFiltered: %d", numEntriesFiltered);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

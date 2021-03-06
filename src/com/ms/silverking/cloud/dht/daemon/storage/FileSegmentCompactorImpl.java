package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.TimeAndVersionRetentionPolicy;
import com.ms.silverking.cloud.dht.ValueRetentionPolicy;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.SegmentIndexLocation;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment.SegmentPrereadMode;
import com.ms.silverking.cloud.dht.daemon.storage.protocol.StorageProtocolUtil;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;

public class FileSegmentCompactorImpl implements FileSegmentCompactor {
  private static final boolean verbose = false;

  private static final String compactionDirName = "compact";
  private static final String trashDirName = "trash";

  /*
   * Current strategy is to leave the segment size constant, and to count on sparse file support for
   * actual disk savings. That is, upon compaction, the data will be compacted at the start of the
   * data segment, and the index will remain at the end of the data segment. In between, sparse
   * file support should allow the file system to omit allocation of bytes.
   */

  private static File getDir(File nsDir, String subDirName) throws IOException {
    File subDir;

    subDir = new File(nsDir, subDirName);
    if (!subDir.exists()) {
      if (!subDir.mkdir()) {
        if (!subDir.exists()) {
          throw new IOException("mkdir failed: " + nsDir + " " + subDir);
        }
      }
    }
    return subDir;
  }

  private static File getCompactionDir(File nsDir) throws IOException {
    return getDir(nsDir, compactionDirName);
  }

  private static File getTrashDir(File nsDir) throws IOException {
    return getDir(nsDir, trashDirName);
  }

  private static File getCompactionFile(File nsDir, int segmentNumber) throws IOException {
    File compactionDir;
    File compactionFile;

    compactionDir = getCompactionDir(nsDir);
    compactionFile = new File(compactionDir, Integer.toString(segmentNumber));
    return compactionFile;
  }

  private static File getTrashFile(File nsDir, int segmentNumber) throws IOException {
    File trashDir;
    File trashFile;

    trashDir = getTrashDir(nsDir);
    trashFile = new File(trashDir, Integer.toString(segmentNumber));
    return trashFile;
  }

  static long readCreationTime(File file) throws IOException {
    BasicFileAttributes view = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
    return view.creationTime().toMillis();
  }

  static void setCreationTime(File file, long millis) throws IOException {
    Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class).setTimes(null, null, FileTime.fromMillis(millis));
  }

  private static void rename(File src, File target) throws IOException {
    long creationTime = readCreationTime(src);
    if (!src.renameTo(target)) {
      throw new IOException("Rename failed: " + src + " " + target);
    }
    setCreationTime(target, creationTime);
  }

  static FileSegment createCompactedSegment(File nsDir, int segmentNumber, NamespaceOptions nsOptions, int segmentSize,
      EntryRetentionCheck retentionCheck, HashedSetMap<DHTKey, Triple<Long, Integer, Long>> removedEntries,
      boolean includeStorageTime) {
    try {
      DataSegmentWalker dsWalker;
      FileSegment sourceSegment;
      FileSegment destSegment;

      sourceSegment = FileSegment.openReadOnly(nsDir, segmentNumber, nsOptions.getSegmentSize(), nsOptions,
          SegmentIndexLocation.RAM, SegmentPrereadMode.Preread);
      destSegment = FileSegment.create(getCompactionDir(nsDir), segmentNumber, nsOptions.getSegmentSize(),
          FileSegment.SyncMode.NoSync, nsOptions);

      dsWalker = new DataSegmentWalker(sourceSegment.dataBuf);
      for (DataSegmentWalkEntry entry : dsWalker) {
        SegmentStorageResult storageResult;
        if (verbose) {
          System.out.println(entry);
        }
        if (!StorageProtocolUtil.storageStateValidForRead(nsOptions.getConsistencyProtocol(),
            entry.getStorageState())) {
          if (verbose) {
            Log.warningf("Ignoring invalid storage state %s %d", entry.getKey(), entry.getStorageState());
          }
        } else {
          if (retentionCheck.shouldRetain(segmentNumber, entry)) {
            if (verbose) {
              System.out.println("setting: " + entry.getOffset());
              System.out.println("sanity check: " + sourceSegment.getPKC().get(entry.getKey()));
              Log.warning("Retaining:\t", entry.getKey());
            }

            storageResult = destSegment.putFormattedValue(entry.getKey(), entry.getStoredFormat(),
                entry.getStorageParameters(), nsOptions);
            if (storageResult != SegmentStorageResult.stored) {
              // FUTURE - think about duplicate stores, and the duplicate store WritableSegmentBase
              if (storageResult != SegmentStorageResult.duplicateStore) {
                throw new RuntimeException("Compaction failed: " + storageResult);
              } else {
                if (Log.levelMet(Level.FINE)) {
                  Log.finef("Duplicate store in compaction %s", entry.getKey());
                }
              }
            }
          } else {
            if (verbose) {
              Log.warning("Dropping: \t", entry.getKey());
            }
            removedEntries.addValue(entry.getKey(),
                new Triple<>(entry.getVersion(), segmentNumber, includeStorageTime ? entry.getCreationTime() : 0));
          }
        }
      }
      return destSegment;
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe, "Unable to compact: " + segmentNumber);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public List<Integer> getTrashSegments(File nsDir) throws IOException {
    try {
      return getSegments(getTrashDir(nsDir));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<Integer> getCompactSegments(File nsDir) throws IOException {
    try {
      return getSegments(getCompactionDir(nsDir));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public HashedSetMap<DHTKey, Triple<Long, Integer, Long>> compact(File nsDir, int segmentNumber,
      NamespaceOptions nsOptions, EntryRetentionCheck retentionCheck, boolean logCompaction) throws IOException {
    FileSegment compactedSegment;
    File oldFile;
    File trashFile;
    File newFile;
    HashedSetMap<DHTKey, Triple<Long, Integer, Long>> removedEntries;

    removedEntries = new HashedSetMap<>();
    if (logCompaction) {
      Log.warning("Compacting segment: ", segmentNumber);
    }
    compactedSegment = createCompactedSegment(nsDir, segmentNumber, nsOptions, nsOptions.getSegmentSize(),
        retentionCheck, removedEntries, nsOptions.getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS);
    if (logCompaction) {
      Log.warning("Done compacting segment: ", segmentNumber);
    }
    compactedSegment.persist();
    if (logCompaction) {
      Log.warning("Swapping to compacted segment: ", segmentNumber);
    }
    oldFile = FileSegment.fileForSegment(nsDir, segmentNumber);
    newFile = getCompactionFile(nsDir, segmentNumber);
    trashFile = getTrashFile(nsDir, segmentNumber);
    rename(oldFile, trashFile); // Leave old file around for one cycle in case there are references to it
    rename(newFile, oldFile);
    if (logCompaction) {
      Log.warning("Done swapping to compacted segment: ", segmentNumber);
    }
    return removedEntries;
  }

  @Override
  public void delete(File nsDir, int segmentNumber) throws IOException {
    File oldFile;
    File trashFile;

    oldFile = FileSegment.fileForSegment(nsDir, segmentNumber);
    trashFile = getTrashFile(nsDir, segmentNumber);
    rename(oldFile, trashFile); // Leave old file around for one cycle in case there are references to it
  }

  @Override
  public int emptyTrashAndCompaction(File nsDir) {
    int totalDeleted;

    totalDeleted = 0;
    try {
      totalDeleted += emptyDir(getTrashDir(nsDir));
      totalDeleted += emptyDir(getCompactionDir(nsDir));
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe);
    }
    return totalDeleted;
  }

  @Override
  public int forceEmptyTrashAndCompaction(File nsDir) throws IOException {
    int totalDeleted;

    totalDeleted = 0;
    totalDeleted += forceEmptyDir(getTrashDir(nsDir));
    totalDeleted += forceEmptyDir(getCompactionDir(nsDir));
    return totalDeleted;
  }

  private static List<Integer> getSegments(File segmentsDir) throws IOException {
    List<Integer> segments;

    segments = new LinkedList<Integer>();
    Files.list(segmentsDir.toPath()).forEach(path -> {
      try {
        Integer segmentNum;

        segmentNum = Integer.parseInt(path.toFile().getName());
        segments.add(segmentNum);
      } catch (NumberFormatException nfe) {
        Log.logErrorWarning(nfe, "Invalid segment file found: " + path);
      }
    });

    return segments;
  }

  private static int emptyDir(File dir) {
    File[] files;

    files = dir.listFiles();
    if (files == null) {
      throw new RuntimeException("[" + dir + "] is not a dir or I/O Exception happened during dir.listFiles()");
    }
    for (File file : files) {
      if (!file.delete()) {
        Log.warning("Failed to delete", file);
      }
    }
    // Can be inaccurate if we can't delete some; not important here
    return files.length;
  }

  private static int forceEmptyDir(File dir) throws IOException {
    int filesDeleted;
    File[] files;

    filesDeleted = 0;
    files = dir.listFiles();
    if (files == null) {
      throw new IOException("[" + dir + "] is not a dir or I/O Exception happened during dir.listFiles()");
    }
    for (File file : files) {
      // Will throw an IOException with diagnostic reporting, when a file cannot be deleted
      Files.delete(file.toPath());
      filesDeleted++;
    }

    return filesDeleted;
  }

  public static void main(String[] args) {
    try {
      if (args.length != 3) {
        System.out.println("args: <nsDir> <segmentNumber> <timeSpanSeconds>");
      } else {
        File nsDir;
        int segmentNumber;
        NamespaceOptions nsOptions;
        ValueRetentionPolicy valueRetentionPolicy;
        int timeSpanSeconds;

        nsDir = new File(args[0]);
        segmentNumber = Integer.parseInt(args[1]);
        timeSpanSeconds = Integer.parseInt(args[2]);
        valueRetentionPolicy = new TimeAndVersionRetentionPolicy(TimeAndVersionRetentionPolicy.Mode.wallClock, 1,
            timeSpanSeconds);
        nsOptions = DHTConstants.defaultNamespaceOptions.valueRetentionPolicy(valueRetentionPolicy);
        new FileSegmentCompactorImpl().compact(nsDir, segmentNumber, nsOptions, new TestRetentionCheck(32768), true);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

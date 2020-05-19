package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.collection.DHTKeyIntEntry;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.SegmentIndexLocation;
import com.ms.silverking.cloud.dht.daemon.storage.FileSegment.SegmentPrereadMode;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

/**
 * Two purposes: 1) provide file segment recovery methods; 2) provide utility methods for reading a file segment.
 */
public class FileSegmentRecoverer {
  private final File nsDir;
  private final NamespaceProperties nsProperties;

  private static final boolean verbose = false;

  public FileSegmentRecoverer(File nsDir, NamespaceProperties nsProperties) {
    this.nsDir = nsDir;
    this.nsProperties = nsProperties;
  }

  FileSegment recoverFullSegment(int segmentNumber, NamespaceStore nsStore, SegmentIndexLocation segmentIndexLocation,
      SegmentPrereadMode segmentPrereadMode) {
    FileSegment _segment;

    Log.warningf("Recovering full segment: %d %s", segmentNumber, segmentPrereadMode);
    _segment = null;
    try {
      FileSegment segment;
      Stopwatch sw;

      sw = new SimpleStopwatch();
      segment = FileSegment.openReadOnly(nsDir, segmentNumber, nsStore.getNamespaceOptions().getSegmentSize(),
          nsStore.getNamespaceOptions(), segmentIndexLocation, segmentPrereadMode);
      for (DHTKeyIntEntry entry : segment.getPKC()) {
        int offset;
        long creationTime;

        offset = entry.getValue();
        if (offset < 0) {
          OffsetList offsetList;

          offsetList = segment.offsetListStore.getOffsetList(-offset);
          for (Triple<Integer, Long, Long> offsetVersionAndStorageTime :
              offsetList.offsetVersionAndStorageTimeIterable()) {
            creationTime = offsetVersionAndStorageTime.getV3();
            nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, offsetVersionAndStorageTime.getV2(),
                creationTime, segment);
          }
        } else {
          long version;

          if (nsStore.getNamespaceOptions().getVersionMode() == NamespaceVersionMode.SINGLE_VERSION) {
            version = DHTConstants.unspecifiedVersion;
          } else {
            version = segment.getVersion(offset);
          }
          if (nsStore.getNamespaceOptions().getRevisionMode() == RevisionMode.UNRESTRICTED_REVISIONS) {
            creationTime = segment.getCreationTime(offset);
          } else {
            creationTime = 0;
          }
          nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, version, creationTime, segment);
        }
      }

      if (segmentPrereadMode != SegmentPrereadMode.Preread) {
        segment.close();
      } else {
        _segment = segment;
      }
      sw.stop();
      Log.warning("Done recovering full segment: ", segmentNumber + "\t" + sw.getElapsedSeconds());
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe, "Unable to recover: " + segmentNumber);
    }
    return _segment;
  }

  FileSegment recoverPartialSegment(int segmentNumber, NamespaceStore nsStore) {
    try {
      DataSegmentWalker dsWalker;
      FileSegment fileSegment;
      DataSegmentWalkEntry lastEntry;
      FileSegment.SyncMode syncMode;

      Log.warning("Recovering partial segment: ", segmentNumber);
      syncMode = nsStore.getNamespaceOptions().getStorageType() == StorageType.FILE_SYNC ?
          FileSegment.SyncMode.Sync :
          FileSegment.SyncMode.NoSync;
      fileSegment = FileSegment.openForRecovery(nsDir, segmentNumber, nsStore.getNamespaceOptions().getSegmentSize(),
          syncMode, nsStore.getNamespaceOptions());
      dsWalker = new DataSegmentWalker(fileSegment.dataBuf);
      lastEntry = null;
      for (DataSegmentWalkEntry entry : dsWalker) {
        if (verbose) {
          System.out.println(entry);
        }
        fileSegment._put(entry.getKey(), entry.getOffset(), entry.getVersion(), entry.getCreator().getBytes(),
            nsStore.getNamespaceOptions());
        if (verbose) {
          System.out.println("setting: " + entry.getOffset());
          System.out.println("sanity check: " + fileSegment.getPKC().get(entry.getKey()));
        }
        lastEntry = entry;
        nsStore.putSegmentNumberAndVersion(entry.getKey(), segmentNumber, entry.getVersion(), entry.getCreationTime(),
            fileSegment);
        nsStore.addToSizeStats(entry.getUncompressedLength(), entry.getCompressedLength());
      }
      if (lastEntry != null) {
        if (verbose) {
          System.out.println(lastEntry);
          System.out.println("setting nextFree: " + lastEntry.nextEntryOffset());
        }
        fileSegment.setNextFree(lastEntry.nextEntryOffset());
      } else {
        fileSegment.setNextFree(SegmentFormat.headerSize);
      }
      Log.warning("Done recovering partial segment: ", segmentNumber);
      return fileSegment;
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe, "Unable to recover partial: " + segmentNumber);
      Log.logErrorWarning(ioe);
      return null;
    }
  }

  /**
   * Read a segment. Utility method only.
   */
  FileSegment readPartialSegment(int segmentNumber, boolean displayEntries) {
    try {
      DataSegmentWalker dsWalker;
      FileSegment fileSegment;
      DataSegmentWalkEntry lastEntry;
      FileSegment.SyncMode syncMode;
      NamespaceOptions nsOptions;

      nsOptions = nsProperties.getOptions();
      Log.warning("Reading partial segment: ", segmentNumber);
      syncMode = nsOptions.getStorageType() == StorageType.FILE_SYNC ?
          FileSegment.SyncMode.Sync :
          FileSegment.SyncMode.NoSync;
      fileSegment = FileSegment.openForRecovery(nsDir, segmentNumber, nsOptions.getSegmentSize(), syncMode, nsOptions);
      dsWalker = new DataSegmentWalker(fileSegment.dataBuf);
      lastEntry = null;
      for (DataSegmentWalkEntry entry : dsWalker) {
        if (displayEntries || verbose) {
          System.out.println(entry);
        }
        fileSegment._put(entry.getKey(), entry.getOffset(), entry.getVersion(), entry.getCreator().getBytes(),
            nsOptions);
        // Utility method. We display, rather than update the nsStore
        if (verbose) {
          System.out.println("setting: " + entry.getOffset());
          System.out.println("sanity check: " + fileSegment.getPKC().get(entry.getKey()));
        }
        lastEntry = entry;
      }
      if (lastEntry != null) {
        if (displayEntries || verbose) {
          System.out.println(lastEntry);
          System.out.println("setting nextFree: " + lastEntry.nextEntryOffset());
        }
        fileSegment.setNextFree(lastEntry.nextEntryOffset());
      } else {
        fileSegment.setNextFree(SegmentFormat.headerSize);
      }
      Log.warning("Done reading partial segment: ", segmentNumber);
      return fileSegment;
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe, "Unable to read partial: " + segmentNumber);
      Log.logErrorWarning(ioe);
      return null;
    }
  }

  /*
   * Utility method only
   */
  void readFullSegment(int segmentNumber, SegmentIndexLocation segmentIndexLocation,
      SegmentPrereadMode segmentPrereadMode) {
    Stopwatch sw;

    Log.warning("Reading full segment: ", segmentNumber);
    sw = new SimpleStopwatch();
    try {
      FileSegment segment;
      NamespaceOptions nsOptions;

      nsOptions = nsProperties.getOptions();
      segment = FileSegment.openReadOnly(nsDir, segmentNumber, nsOptions.getSegmentSize(), nsOptions,
          segmentIndexLocation, segmentPrereadMode);
      for (DHTKeyIntEntry entry : segment.getPKC()) {
        int offset;

        offset = entry.getValue();
        if (offset < 0) {
          OffsetList offsetList;

          offsetList = segment.offsetListStore.getOffsetList(-offset);
          for (int listOffset : offsetList) {
            // Utility method. We display, rather than update the nsStore
            System.out.printf("%s\t%d\t%d\t%f\t*%d\t%d\n", entry, segment.getCreationTime(offset),
                segment.getVersion(listOffset), KeyUtil.keyEntropy(entry), offset, offsetList);
          }
        } else {
          // Utility method. We display, rather than update the nsStore
          System.out.printf("%s\t%d\t%d\t%f\t%d\n", entry, segment.getCreationTime(offset), segment.getVersion(offset),
              KeyUtil.keyEntropy(entry), offset);
        }
      }
      // Utility method. We display, rather than update the nsStore stats here

      segment.close();
      sw.stop();
      Log.warning("Done reading full segment: ", segmentNumber + " " + sw);
    } catch (IOException ioe) {
      Log.logErrorWarning(ioe, "Unable to recover: " + segmentNumber);
    }
  }

  public static void main(String[] args) {
    // This tool runs stand-alone with dependency of "properties" file, which only used by NSP mode for now
    if (DHTConfiguration.defaultNamespaceOptionsMode != NamespaceOptionsMode.MetaNamespace) {
      throw new IllegalArgumentException(
          "You're in the default mode of [" + DHTConfiguration.defaultNamespaceOptionsMode + "], which is not " +
              "supported by this tool");
    }

    try {
      if (args.length < 2 || args.length > 4) {
        System.out.println("args: <nsDir> <segmentNumber> [maxSegmentNumber] [persist]");
        return;
      } else {
        FileSegmentRecoverer segmentReader;
        File nsDir;
        int minSegmentNumber;
        int maxSegmentNumber;
        boolean persist;

        nsDir = new File(args[0]);
        minSegmentNumber = Integer.parseInt(args[1]);
        if (args.length >= 3) {
          maxSegmentNumber = Integer.parseInt(args[2]);
        } else {
          maxSegmentNumber = minSegmentNumber;
        }
        if (args.length >= 4) {
          persist = Boolean.parseBoolean(args[3]);
        } else {
          persist = false;
        }

        segmentReader = new FileSegmentRecoverer(nsDir, NamespacePropertiesIO.read(nsDir));
        FileSegment segment;

        for (int segmentNumber = minSegmentNumber; segmentNumber <= maxSegmentNumber; segmentNumber++) {
          try {
            segmentReader.readFullSegment(segmentNumber, SegmentIndexLocation.RAM, SegmentPrereadMode.Preread);
          } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Exception reading full segment. Reading partial.\n");
            segment = segmentReader.readPartialSegment(segmentNumber, true);
            if (persist) {
              segment.persist();
            }
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

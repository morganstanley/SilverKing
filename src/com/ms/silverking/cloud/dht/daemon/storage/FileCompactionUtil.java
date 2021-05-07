package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.List;

import com.ms.silverking.io.FileUtil;
import com.ms.silverking.log.Log;

class FileCompactionUtil {
  private static final boolean verbose = false;

  // FUTURE - This class, and compaction in general, are in the middle of cleaning/refactoring

  private static final String compactionDirName = "compact";
  private static final String trashDirName = "trash";

  public static File getDir(File nsDir, String subDirName) throws IOException {
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

  static File getCompactionDir(File nsDir) throws IOException {
    return getDir(nsDir, compactionDirName);
  }

  static File getTrashDir(File nsDir) throws IOException {
    return getDir(nsDir, trashDirName);
  }

  static File getCompactionFile(File nsDir, int segmentNumber) throws IOException {
    File compactionDir;
    File compactionFile;

    compactionDir = getCompactionDir(nsDir);
    compactionFile = new File(compactionDir, Integer.toString(segmentNumber));
    return compactionFile;
  }

  static long readCreationTime(File file) throws IOException {
    BasicFileAttributes view = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
    return view.creationTime().toMillis();
  }

  static void setCreationTime(File file, long millis) throws IOException {
    Files.getFileAttributeView(file.toPath(), BasicFileAttributeView.class).setTimes(null, null, FileTime.fromMillis(millis));
  }

  static void rename(File src, File target) throws IOException {
    long creationTime;

    creationTime = readCreationTime(src);
    if (!src.renameTo(target)) { // Linux semantics expected; Windows will fail
      throw new IOException("Rename failed: " + src + " " + target);
    }
    setCreationTime(target, creationTime);
  }

  public static List<Integer> getTrashSegments(File nsDir) throws IOException {
    try {
      return FileUtil.numericFilesInDirAsSortedIntegerList(getTrashDir(nsDir));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static List<Integer> getCompactSegments(File nsDir) throws IOException {
    try {
      return FileUtil.numericFilesInDirAsSortedIntegerList(getCompactionDir(nsDir));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static void delete(File nsDir, int segmentNumber) {
    File oldFile;

    oldFile = FileSegment.fileForSegment(nsDir, segmentNumber);
    if (!oldFile.delete()) {
      Log.warningf("Unable to delete %s", oldFile);
    }
  }
}

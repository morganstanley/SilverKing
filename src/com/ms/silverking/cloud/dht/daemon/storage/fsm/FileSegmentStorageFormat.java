package com.ms.silverking.cloud.dht.daemon.storage.fsm;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.io.FileUtil;
import com.ms.silverking.log.Log;

public class FileSegmentStorageFormat {
  private final int storageFormat;

  private static final String fileName = "storageFormat";
  private static final int defaultStorageFormat = 0;

  /*
   * Storage formats:
   * 0 - Base format. Data + offset lists
   * 1 - Adds invalidation index
   * 2 - Puts data segment in ltv format?
   */

  public FileSegmentStorageFormat(int storageFormat) {
    this.storageFormat = Math.max(storageFormat, 0);
  }

  public boolean dataSegmentIsLTV() {
    return storageFormat >= 2;
  }

  public boolean metaDataIsLTV() {
    return storageFormat >= 1;
  }

  public boolean containsFSMHeader() {
    return storageFormat >= 1;
  }

  public boolean invalidationsAreIndexed() {
    return storageFormat >= 1;
  }

  ///////////////////

  private static File getFile(File dir) {
    return new File(dir, fileName);
  }

  public static FileSegmentStorageFormat read(File dir) throws IOException {
    File f;

    f = getFile(dir);
    return new FileSegmentStorageFormat(f.exists() ? FileUtil.readFileAsInt(f) : 0);
  }

  public static void write(File dir, int storageFormat) throws IOException {
    FileUtil.writeToFile(getFile(dir), Integer.toString(storageFormat));
  }

  public static FileSegmentStorageFormat parse(String storageFormat) {
    if (storageFormat == null) {
      return new FileSegmentStorageFormat(defaultStorageFormat);
    } else {
      try {
        return new FileSegmentStorageFormat(Integer.parseInt(storageFormat));
      } catch (Exception e) {
        Log.logErrorWarning(e, "Using defaultStorageFormat: " + defaultStorageFormat);
        return new FileSegmentStorageFormat(defaultStorageFormat);
      }
    }
  }

  @Override
  public String toString() {
    return Integer.toString(storageFormat);
  }
}

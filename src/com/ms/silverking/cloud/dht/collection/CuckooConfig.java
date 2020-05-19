package com.ms.silverking.cloud.dht.collection;

import java.nio.ByteBuffer;

import com.ms.silverking.code.Constraint;
import com.ms.silverking.numeric.NumConversion;

public class CuckooConfig {
  // specified
  private final int totalEntries;
  private final int numSubTables;
  private final int entriesPerBucket;
  // derived
  private final int numSubTableBuckets;

  private static final int totalEntriesOffset = 0;
  private static final int numSubTablesOffset = totalEntriesOffset + NumConversion.BYTES_PER_INT;
  private static final int entriesPerBucketOffset = numSubTablesOffset + NumConversion.BYTES_PER_INT;
  public static final int BYTES = entriesPerBucketOffset + NumConversion.BYTES_PER_INT;

  public CuckooConfig(int totalEntries, int numSubTables, int entriesPerBucket) {
    Constraint.ensureNonZero(totalEntries);
    Constraint.ensureNonZero(numSubTables);
    Constraint.ensureNonZero(entriesPerBucket);
    this.totalEntries = totalEntries;
    this.numSubTables = numSubTables;
    this.entriesPerBucket = entriesPerBucket;
    if (totalEntries < numSubTables * entriesPerBucket) {
      throw new RuntimeException("Invalid configuration: totalEntries < numSubTables * entriesPerBucket");
    }
    if (totalEntries % (numSubTables * entriesPerBucket) != 0) {
      throw new RuntimeException("Invalid configuration: totalEntries % (numSubTables * entriesPerBucket) != 0");
    }
    numSubTableBuckets = totalEntries / (numSubTables * entriesPerBucket);
    sanityCheck();
  }

  private void sanityCheck() {
    if (numSubTables < 2) {
      throw new RuntimeException("numSubTables must be >= 2");
    }
    if (Integer.bitCount(entriesPerBucket) != 1) {
      throw new RuntimeException("Supplied entriesPerBucket must be a perfect power of 2");
    }
    if (Integer.bitCount(numSubTables) != 1) {
      throw new RuntimeException("Supplied numSubTables must be a perfect power of 2");
    }
  }

  public int getTotalEntries() {
    return totalEntries;
  }

  public int getNumSubTables() {
    return numSubTables;
  }

  public int getEntriesPerBucket() {
    return entriesPerBucket;
  }

  public int getNumSubTableBuckets() {
    return numSubTableBuckets;
  }

  public void persist(ByteBuffer buf) {
    persist(buf, 0);
  }

  public void persist(ByteBuffer buf, int offset) {
    buf.putInt(offset + totalEntriesOffset, totalEntries);
    buf.putInt(offset + numSubTablesOffset, numSubTables);
    buf.putInt(offset + entriesPerBucketOffset, entriesPerBucket);
  }

  public static CuckooConfig read(ByteBuffer buf) {
    return read(buf, 0);
  }

  public static CuckooConfig read(ByteBuffer buf, int offset) {
    int totalEntries;
    int numSubTables;
    int entriesPerBucket;

    totalEntries = buf.getInt(offset + totalEntriesOffset);
    numSubTables = buf.getInt(offset + numSubTablesOffset);
    entriesPerBucket = buf.getInt(offset + entriesPerBucketOffset);
    return new CuckooConfig(totalEntries, numSubTables, entriesPerBucket);
  }

  @Override
  public String toString() {
    return totalEntries + ":" + numSubTables + ":" + entriesPerBucket + ":" + numSubTableBuckets;
  }
}

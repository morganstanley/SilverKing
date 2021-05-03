package com.ms.silverking.cloud.dht.collection;

import com.ms.silverking.numeric.NumConversion;

/**
 * Reads/writes PartialKeyCuckoo maps (which are RAM-base.)
 * Deprecated: usage of a single constant remains
 */
public class PKCReaderWriter {
  public static final int overheadBytes = NumConversion.BYTES_PER_INT * 4;
}

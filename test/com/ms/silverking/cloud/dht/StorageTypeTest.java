package com.ms.silverking.cloud.dht;

import static com.ms.silverking.cloud.dht.StorageType.FILE;
import static com.ms.silverking.cloud.dht.StorageType.FILE_SYNC;
import static com.ms.silverking.cloud.dht.StorageType.RAM;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class StorageTypeTest {

  @Test
  public void testIsFileBased() {
    Object[][] testCases = { { RAM, false }, { FILE, true }, { FILE_SYNC, true }, };

    for (Object[] testCase : testCases) {
      StorageType type = (StorageType) testCase[0];
      boolean expected = (boolean) testCase[1];

      assertEquals(expected, type.isFileBased());
    }
  }

}

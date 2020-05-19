package com.ms.silverking.cloud.dht.client.test;

import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.ThreadUtil;

public class LockTest extends BaseClientTest {
  public static final String testName = "LockTest";

  private static final String testKey = "k1";
  private static final String testKey2 = "k2";
  private static final String initialValue = "v1.1";
  private static final String failedPutValue = "v1.2";
  private static final String modifiedValue = "v1.3";
  private static final String modifiedValue2 = "v1.4";

  public LockTest() {
    super(testName);
  }

  @Override
  public Pair<Integer, Integer> runTest(DHTSession session, Namespace ns) {
    SynchronousNamespacePerspective<String, String> syncNSP;

    syncNSP = ns.openSyncPerspective(ns.getDefaultNSPOptions(String.class, String.class));
    return testLock(syncNSP);
  }

  public Pair<Integer, Integer> testLock(SynchronousNamespacePerspective<String, String> syncNSP) {
    int successful;
    int failed;

    successful = 0;
    failed = 0;
    try {
      PutOptions defaultPutOptions;
      InvalidationOptions defaultInvalidationOptions;

      defaultPutOptions = syncNSP.getNamespace().getOptions().getDefaultPutOptions();
      defaultInvalidationOptions = syncNSP.getNamespace().getOptions().getDefaultInvalidationOptions();
      System.out.println("Writing");
      syncNSP.put(testKey, initialValue, defaultPutOptions.lockSeconds((short) 2));

      // Test the advisory lock
      System.out.println("Writing - should fail");
      try {
        syncNSP.put(testKey, failedPutValue, defaultPutOptions.lockSeconds((short) 2));
      } catch (PutException pe) {
        if (pe.getFailureCause(testKey) == FailureCause.LOCKED) {
          System.out.println("Correctly locked");
        } else {
          throw new RuntimeException("Unexpected failure", pe);
        }
      }
      checkValue(syncNSP, testKey, initialValue);

      // Test lock timeout
      ThreadUtil.sleepSeconds(3);
      syncNSP.put(testKey, modifiedValue, defaultPutOptions.lockSeconds((short) 1));
      checkValue(syncNSP, testKey, modifiedValue);

      // Test that lack of advisory lock use allows writing
      syncNSP.put(testKey, modifiedValue2, defaultPutOptions.lockSeconds((short) 0));
      checkValue(syncNSP, testKey, modifiedValue2);

      syncNSP.put(testKey2, initialValue, defaultPutOptions.lockSeconds((short) 0));
      syncNSP.invalidate(testKey2, defaultInvalidationOptions.lockSeconds((short) 60));
      try {
        syncNSP.put(testKey2, modifiedValue, defaultPutOptions.lockSeconds((short) 62));
        throw new RuntimeException("Unexpected write to locked value");
      } catch (PutException pe) {
        if (pe.getFailureCause(testKey2) == FailureCause.LOCKED) {
          System.out.println("Correctly locked2");
        } else {
          throw new RuntimeException("Unexpected failure", pe);
        }
      }
      checkValue(syncNSP, testKey2, null);

      ++successful;
    } catch (Exception e) {
      Log.logErrorWarning(e, "testLock failed");
      ++failed;
    }
    return Pair.of(successful, failed);
  }
}

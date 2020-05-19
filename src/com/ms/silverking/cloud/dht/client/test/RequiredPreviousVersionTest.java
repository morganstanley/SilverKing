package com.ms.silverking.cloud.dht.client.test;

import java.util.HashSet;
import java.util.Set;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.FailureCause;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.log.Log;

public class RequiredPreviousVersionTest extends BaseClientTest {
  public static final String testName = "RequiredPreviousVersionTest";

  private static final String testKey = "k1";
  private static final String initialValue = "v1.0";
  private static final String failedPutValue = "v1.f";
  private static final String modifiedValue = "v1.m";
  private static final String modifiedValue2 = "v1.2";
  private static final String modifiedValue3 = "v1.3";
  private static final String modifiedValue4 = "v1.4";
  private static final String modifiedValue5 = "v1.5";
  private static final String modifiedValue6 = "v1.6";

  private static final String testKey2 = "k2";

  private static final Set<Triple<ConsistencyProtocol, NamespaceVersionMode, RevisionMode>> nsop;

  static {
    nsop = new HashSet<>();
    nsop.add(new Triple(ConsistencyProtocol.TWO_PHASE_COMMIT, NamespaceVersionMode.CLIENT_SPECIFIED,
        RevisionMode.NO_REVISIONS));
    //nsop.add(new Triple(ConsistencyProtocol.TWO_PHASE_COMMIT, NamespaceVersionMode.CLIENT_SPECIFIED, RevisionMode
    // .UNRESTRICTED_REVISIONS));
    nsop.add(new Triple(ConsistencyProtocol.LOOSE, NamespaceVersionMode.CLIENT_SPECIFIED, RevisionMode.NO_REVISIONS));
    //nsop.add(new Triple(ConsistencyProtocol.LOOSE, NamespaceVersionMode.CLIENT_SPECIFIED, RevisionMode
    // .UNRESTRICTED_REVISIONS));
    // FIXME - define semantics of required previous version for unrestricted revisions
  }

  public RequiredPreviousVersionTest() {
    super(testName, nsop);
  }

  @Override
  public Pair<Integer, Integer> runTest(DHTSession session, Namespace ns) {
    SynchronousNamespacePerspective<String, String> syncNSP;

    syncNSP = ns.openSyncPerspective(ns.getDefaultNSPOptions(String.class, String.class));
    return testRequiredPreviousVersion(syncNSP);
  }

  public Pair<Integer, Integer> testRequiredPreviousVersion(SynchronousNamespacePerspective<String, String> syncNSP) {
    int successful;
    int failed;

    successful = 0;
    failed = 0;
    try {
      PutOptions defaultPutOptions;
      InvalidationOptions defaultInvalidationOptions;

      defaultPutOptions = syncNSP.getNamespace().getOptions().getDefaultPutOptions();
      defaultInvalidationOptions = syncNSP.getNamespace().getOptions().getDefaultInvalidationOptions();

      checkValue(syncNSP, testKey, null);

      System.out.println("Writing");
      syncNSP.put(testKey, initialValue, defaultPutOptions.version(1));
      checkValueAndVersion(syncNSP, testKey, initialValue, 1);

      System.out.println("Writing - should fail due to version <= requiredPreviousVersion");
      try {
        syncNSP.put(testKey, failedPutValue, defaultPutOptions.requiredPreviousVersion(2).version(2));
        throw new RuntimeException("Failed to generate IllegalArgumentException");
      } catch (IllegalArgumentException iae) {
        // Expected
        System.out.println("Correctly generated IllegalArgumentException");
      }
      checkValueAndVersion(syncNSP, testKey, initialValue, 1);

      System.out.println("Writing - should fail due to requiredPreviousVersion not met");
      try {
        syncNSP.put(testKey, failedPutValue, defaultPutOptions.requiredPreviousVersion(5).version(6));
      } catch (PutException pe) {
        if (pe.getFailureCause(testKey) == FailureCause.INVALID_VERSION) {
          System.out.println("Correctly detected invalid version");
        } else {
          throw new RuntimeException("Unexpected failure", pe);
        }
      }
      checkValueAndVersion(syncNSP, testKey, initialValue, 1);

      // Test requiredPreviousVersion
      syncNSP.put(testKey, modifiedValue, defaultPutOptions.requiredPreviousVersion(1).version(2));
      checkValueAndVersion(syncNSP, testKey, modifiedValue, 2);

      // Test requiredPreviousVersion
      syncNSP.put(testKey, modifiedValue2, defaultPutOptions.requiredPreviousVersion(2).version(3));
      checkValueAndVersion(syncNSP, testKey, modifiedValue2, 3);

      // key2 tests

      checkValue(syncNSP, testKey2, null);

      // Test requiredPreviousVersion
      syncNSP.put(testKey2, initialValue,
          defaultPutOptions.requiredPreviousVersion(PutOptions.previousVersionNonexistent).version(1));
      checkValueAndVersion(syncNSP, testKey2, initialValue, 1);

      // Test requiredPreviousVersion
      try {
        syncNSP.put(testKey2, failedPutValue,
            defaultPutOptions.requiredPreviousVersion(PutOptions.previousVersionNonexistent).version(100));
      } catch (PutException pe) {
        if (pe.getFailureCause(testKey2) == FailureCause.INVALID_VERSION) {
          System.out.println("Correctly detected invalid version");
        } else {
          throw new RuntimeException("Unexpected failure", pe);
        }
      }
      checkValueAndVersion(syncNSP, testKey2, initialValue, 1);

      // Test requiredPreviousVersion
      try {
        syncNSP.put(testKey2, failedPutValue,
            defaultPutOptions.requiredPreviousVersion(PutOptions.previousVersionNonexistentOrInvalid).version(100));
      } catch (PutException pe) {
        if (pe.getFailureCause(testKey2) == FailureCause.INVALID_VERSION) {
          System.out.println("Correctly detected invalid version");
        } else {
          throw new RuntimeException("Unexpected failure", pe);
        }
      }
      checkValueAndVersion(syncNSP, testKey2, initialValue, 1);

      // Test requiredPreviousVersion
      syncNSP.put(testKey2, modifiedValue, defaultPutOptions.requiredPreviousVersion(1).version(2));
      checkValueAndVersion(syncNSP, testKey2, modifiedValue, 2);

      // Test requiredPreviousVersion
      syncNSP.invalidate(testKey2, defaultInvalidationOptions.requiredPreviousVersion(2).version(3));
      checkInvalidatedVersion(syncNSP, testKey2, 3);

      // Test requiredPreviousVersion
      syncNSP.invalidate(testKey2, defaultInvalidationOptions.requiredPreviousVersion(3).version(4));
      checkInvalidatedVersion(syncNSP, testKey2, 4);

      // Test requiredPreviousVersion
      try {
        syncNSP.put(testKey2, failedPutValue,
            defaultPutOptions.requiredPreviousVersion(PutOptions.previousVersionNonexistent).version(5));
      } catch (PutException pe) {
        if (pe.getFailureCause(testKey2) == FailureCause.INVALID_VERSION) {
          System.out.println("Correctly detected invalid version");
        } else {
          throw new RuntimeException("Unexpected failure", pe);
        }
      }
      checkInvalidatedVersion(syncNSP, testKey2, 4);

      syncNSP.put(testKey2, modifiedValue2,
          defaultPutOptions.requiredPreviousVersion(PutOptions.previousVersionNonexistentOrInvalid).version(6));
      checkValueAndVersion(syncNSP, testKey2, modifiedValue2, 6);

      ++successful;
    } catch (Exception e) {
      if (e instanceof PutException) {
        PutException pe;

        pe = (PutException) e;
        Log.warning(pe.getDetailedFailureMessage());
      }
      Log.logErrorWarning(e, "testRequiredPreviousVersion failed");
      ++failed;
    }
    return Pair.of(successful, failed);
  }
}

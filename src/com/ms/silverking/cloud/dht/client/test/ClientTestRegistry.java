package com.ms.silverking.cloud.dht.client.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.common.DHTConstants;

public class ClientTestRegistry {
  private static final Map<String, ClientTest> tests;

  static {
    tests = new HashMap<>();
    tests.put(LockTest.testName, new LockTest());
    tests.put(RequiredPreviousVersionTest.testName, new RequiredPreviousVersionTest());
    tests.put(LooseWriteReadTest.testName, new LooseWriteReadTest());

    for (NamespaceVersionMode vm : NamespaceVersionMode.values()) {
      for (RevisionMode rm : RevisionMode.values()) {
        for (ConsistencyProtocol cp : ConsistencyProtocol.values()) {
          /**/
          addParameterizedNamespaceTest(tests, DHTConstants.defaultNamespaceOptions.versionMode(vm).revisionMode(
              rm).consistencyProtocol(cp).storageFormat("0"));
          addParameterizedNamespaceTest(tests, DHTConstants.defaultNamespaceOptions.versionMode(vm).revisionMode(
              rm).consistencyProtocol(cp).storageFormat("2"));
          /**/
                    /*
                    if (cp == ConsistencyProtocol.LOOSE) {
                        // skip - old versions can't handle rewrites of loose
                    } else {
                        if (vm == NamespaceVersionMode.SINGLE_VERSION && rm == RevisionMode.NO_REVISIONS) {
                            // single version/no revision not supported for backward compatibility (old server
                            recovering data from new server)
                        } else {
                            addParameterizedNamespaceTest(tests, DHTConstants.defaultNamespaceOptions.versionMode(vm)
                            .revisionMode(rm).consistencyProtocol(cp));
                        }
                    }
                    */
        }
      }
    }
  }

  private static void addParameterizedNamespaceTest(Map<String, ClientTest> tests, NamespaceOptions nsOptions) {
    ParameterizedNamespaceTest pt;

    pt = new ParameterizedNamespaceTest(nsOptions);
    tests.put(pt.getTestName(), pt);
  }

  public static List<ClientTest> getTests(Collection<String> names) {
    List<ClientTest> tests;

    tests = new ArrayList<>();
    for (String testName : ImmutableSet.copyOf(names)) {
      tests.add(getTest(testName));
    }
    return ImmutableList.copyOf(tests);
  }

  public static ClientTest getTest(String name) {
    return tests.get(name);
  }

  public static List<String> getAllTestNames() {
    return ImmutableList.copyOf(tests.keySet());
  }

  public static List<ClientTest> getAllTests() {
    return ImmutableList.copyOf(tests.values());
  }
}

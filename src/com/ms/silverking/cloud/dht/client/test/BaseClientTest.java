package com.ms.silverking.cloud.dht.client.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.collection.Triple;
import com.ms.silverking.object.ObjectUtil;

public abstract class BaseClientTest implements ClientTest {
  private final String testName;
  private final Set<Triple<ConsistencyProtocol, NamespaceVersionMode, RevisionMode>> nsOptionsParameters;

  public BaseClientTest(String testName,
      Set<Triple<ConsistencyProtocol, NamespaceVersionMode, RevisionMode>> nsOptionsParameters) {
    this.testName = testName;
    this.nsOptionsParameters = ImmutableSet.copyOf(nsOptionsParameters);
  }

  public BaseClientTest(String testName, ConsistencyProtocol consistencyProtocol, NamespaceVersionMode nsVersionMode,
      RevisionMode revisionMode) {
    this(testName, ImmutableSet.of(new Triple<>(consistencyProtocol, nsVersionMode, revisionMode)));
  }

  public BaseClientTest(String testName) {
    this(testName, ConsistencyProtocol.TWO_PHASE_COMMIT, NamespaceVersionMode.SYSTEM_TIME_NANOS,
        RevisionMode.NO_REVISIONS);
  }

  @Override
  public String getTestName() {
    return testName;
  }

  public List<NamespaceOptions> getNamespaceOptions(NamespaceOptions sessionDefaultNSOptions) {
    List<NamespaceOptions> nsOptions;

    nsOptions = new ArrayList<>();
    for (Triple<ConsistencyProtocol, NamespaceVersionMode, RevisionMode> nsop : nsOptionsParameters) {
      nsOptions.add(sessionDefaultNSOptions.consistencyProtocol(nsop.getV1()).versionMode(nsop.getV2()).revisionMode(
          nsop.getV3()));
    }
    return ImmutableList.copyOf(nsOptions);
  }

  protected void checkValue(SynchronousNamespacePerspective<String, String> syncNSP, String key, String expectedValue)
      throws RetrievalException {
    String value;

    System.out.printf("Reading %s\n", key);
    value = syncNSP.get(key);
    System.out.printf("value: %s\n", value);
    if (!ObjectUtil.equal(value, expectedValue)) {
      throw new RuntimeException(String.format("Expected: %s\tFound: %s", expectedValue, value));
    }
  }

  protected void checkValueAndVersion(SynchronousNamespacePerspective<String, String> syncNSP, String key,
      String expectedValue, long expectedVersion) throws RetrievalException {
    StoredValue<String> sv;
    String value;

    System.out.printf("Reading %s\n", key);
    sv = syncNSP.retrieve(key,
        syncNSP.getOptions().getDefaultGetOptions().retrievalType(RetrievalType.VALUE_AND_META_DATA));
    value = sv.getValue();
    System.out.printf("value: %s\tversion %d\n", value, sv.getMetaData().getVersion());
    if (sv.getMetaData().getVersion() != expectedVersion) {
      throw new RuntimeException(
          String.format("Expected version: %d\tFound: %d", expectedVersion, sv.getMetaData().getVersion()));
    }
    if (!ObjectUtil.equal(value, expectedValue)) {
      throw new RuntimeException(String.format("Expected: %s\tFound: %s", expectedValue, value));
    }
  }

  protected void checkInvalidatedVersion(SynchronousNamespacePerspective<String, String> syncNSP, String key,
      long expectedVersion) throws RetrievalException {
    StoredValue<String> sv;

    System.out.printf("Reading %s\n", key);
    sv = syncNSP.retrieve(key,
        syncNSP.getOptions().getDefaultGetOptions().retrievalType(RetrievalType.META_DATA).returnInvalidations(true));
    System.out.printf("isInvalidation %s version %d\n", sv.getMetaData().isInvalidation(),
        sv.getMetaData().getVersion());
    if (!sv.getMetaData().isInvalidation()) {
      throw new RuntimeException("Expected invalidation");
    }
    if (sv.getMetaData().getVersion() != expectedVersion) {
      throw new RuntimeException(
          String.format("Expected version: %d\tFound: %d", expectedVersion, sv.getMetaData().getVersion()));
    }
  }
}

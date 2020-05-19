package com.ms.silverking.cloud.dht.meta;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.collection.CollectionUtil;

public class Diagnosis {
  private long diagnosisTime;
  private final Set<Disease> diseases;

  public Diagnosis(Set<Disease> diseases, long diagnosisTime) {
    this.diseases = ImmutableSet.copyOf(diseases);
  }

  public Diagnosis(Set<Disease> diseases) {
    this(diseases, SystemTimeUtil.skSystemTimeSource.absTimeMillis());
  }

  public long getDiagnosisTimeMillis() {
    return diagnosisTime;
  }

  public Set<Disease> getDiseases() {
    return diseases;
  }

  public boolean isHealthy() {
    return diseases.size() == 0;
  }

  public boolean isRestartable() {
    return diseases.size() == 1 && diseases.contains(Disease.NoNodeDaemon);
  }

  @Override
  public int hashCode() {
    return Long.hashCode(diagnosisTime) ^ diseases.hashCode();
  }

  @Override
  public String toString() {
    return diagnosisTime + ":" + CollectionUtil.toString(diseases, ',');
  }
}

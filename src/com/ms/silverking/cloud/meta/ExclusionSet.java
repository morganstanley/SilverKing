package com.ms.silverking.cloud.meta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.net.IPAndPort;

public class ExclusionSet extends ServerSetExtension {
  private ExclusionSet(ServerSet serverSet, long mzxid) {
    super(serverSet);
    this.mzxid = mzxid;
  }

  public ExclusionSet(ServerSet serverSet) {
    this(serverSet, INVALID_ZXID);
  }

  private ExclusionSet(long version) {
    this(new ServerSet(new HashSet<>(), version));
  }

  public ExclusionSet(Set<String> excludedEntities, long version, long mzxid) {
    this(new ServerSet(excludedEntities, version), mzxid);
  }

  public static ExclusionSet emptyExclusionSet(long version) {
    return new ExclusionSet(version);
  }

  @Override
  public ExclusionSet addByIPAndPort(Set<IPAndPort> newExcludedEntities) {
    return (ExclusionSet) super.addByIPAndPort(newExcludedEntities);
  }

  @Override
  public ExclusionSet add(Set<String> newExcludedEntities) {
    return new ExclusionSet(serverSet.add(newExcludedEntities));
  }

  @Override
  public ExclusionSet removeByIPAndPort(Set<IPAndPort> newExcludedEntities) {
    return (ExclusionSet) super.removeByIPAndPort(newExcludedEntities);
  }

  @Override
  public ExclusionSet remove(Set<String> newExcludedEntities) {
    return new ExclusionSet(serverSet.remove(newExcludedEntities));
  }

  public static ExclusionSet parse(String def) {
    return new ExclusionSet(
        new ServerSet(CollectionUtil.parseSet(def, singleLineDelimiter), VersionedDefinition.NO_VERSION));
  }

  public static ExclusionSet parse(File file) throws IOException {
    return new ExclusionSet(ServerSet.parse(new FileInputStream(file), VersionedDefinition.NO_VERSION));
  }

  public static ExclusionSet union(ExclusionSet s1, ExclusionSet s2) {
    ExclusionSet u;

    u = emptyExclusionSet(0);
    u = u.add(s1.getServers());
    u = u.add(s2.getServers());
    return u;
  }
}

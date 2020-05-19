package com.ms.silverking.cloud.meta;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * Servers that passively participate in the DHT. These servers may
 * communicate with clients, but do not actively store any data.
 * <p>
 * FUTURE - this class is deprecated; remove
 */
public class PassiveNodeSet extends ServerSetExtension {
  private static final Set<String> emptyStringSet = ImmutableSet.of();
  private static final PassiveNodeSet _emptySet = new PassiveNodeSet(emptyStringSet, VersionedDefinition.NO_VERSION);

  public PassiveNodeSet(ServerSet serverSet) {
    super(serverSet);
  }

  public PassiveNodeSet(Set<String> servers, long version) {
    this(new ServerSet(servers, version));
  }

  public PassiveNodeSet add(Set<String> newServers) {
    return new PassiveNodeSet(serverSet.add(newServers));
  }

  public static PassiveNodeSet parse(File file) throws IOException {
    return new PassiveNodeSet(ServerSet.parse(new FileInputStream(file), VersionedDefinition.NO_VERSION));
  }

  public static PassiveNodeSet parse(String def) throws IOException {
    return new PassiveNodeSet(
        ServerSet.parse(new ByteArrayInputStream(def.getBytes()), VersionedDefinition.NO_VERSION));
  }

  public static PassiveNodeSet emptySet() {
    return _emptySet;
  }
}

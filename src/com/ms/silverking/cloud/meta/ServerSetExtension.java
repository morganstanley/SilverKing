package com.ms.silverking.cloud.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.net.IPAndPort;

/**
 * Common functionality used by ExclusionSet to wrap ServerSet.
 */
public abstract class ServerSetExtension implements VersionedDefinition {
  public static final long INVALID_ZXID = -1;
  public static final String singleLineDelimiter = ",";

  protected final ServerSet serverSet;
  protected long mzxid;

  ServerSetExtension(ServerSet serverSet) {
    this.serverSet = serverSet;
  }

  public long getMzxid() {
    return mzxid;
  }

  public int size() {
    return serverSet.size();
  }

  public Set<String> getServers() {
    return serverSet.getServers();
  }

  @Override
  public long getVersion() {
    return serverSet.getVersion();
  }

  public boolean contains(String serverID) {
    return serverSet.contains(serverID);
  }

  @Override
  public boolean equals(Object o) {
    return serverSet.equals(((ServerSetExtension) o).serverSet);
  }

  @Override
  public String toString() {
    return serverSet.toString();
  }

  public ServerSetExtension addByIPAndPort(Set<IPAndPort> newWorrisomeEntities) {
    Set<String> s;

    s = new HashSet<>();
    for (IPAndPort e : newWorrisomeEntities) {
      s.add(e.getIPAsString());
    }
    return add(s);
  }

  public abstract ServerSetExtension add(Set<String> newExcludedEntities);

  public ServerSetExtension removeByIPAndPort(Set<IPAndPort> newWorrisomeEntities) {
    Set<String> s;

    s = new HashSet<>();
    for (IPAndPort e : newWorrisomeEntities) {
      s.add(e.getIPAsString());
    }
    return remove(s);
  }

  public abstract ServerSetExtension remove(Set<String> newExcludedEntities);

  public Set<IPAndPort> asIPAndPortSet(int port) {
    Set<IPAndPort> s;

    s = new HashSet<>();
    for (String server : serverSet.getServers()) {
      s.add(new IPAndPort(server, port));
    }
    return ImmutableSet.copyOf(s);
  }

  public List<Node> filter(List<Node> raw) {
    List<Node> filtered;

    filtered = new ArrayList<>(raw.size());
    for (Node node : raw) {
      if (!getServers().contains(node.getIDString())) {
        filtered.add(node);
      }
    }
    return filtered;
  }

  public List<IPAndPort> filterByIP(Collection<IPAndPort> raw) {
    List<IPAndPort> filtered;

    filtered = new ArrayList<>(raw.size());
    for (IPAndPort node : raw) {
      boolean worrisome;

      worrisome = false;
      for (String server : getServers()) {
        if (node.getIPAsString().equals(server)) {
          worrisome = true;
          break;
        }
      }
      if (!worrisome) {
        filtered.add(node);
      }
    }
    return filtered;
  }
}

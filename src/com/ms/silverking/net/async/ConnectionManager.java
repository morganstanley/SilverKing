package com.ms.silverking.net.async;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.collection.Pair;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

class ConnectionManager<T extends Connection> implements ConnectionController {

  private static final Set<ConnectionManager> activeManagers = Collections.newSetFromMap(
      new WeakHashMap<ConnectionManager, Boolean>());

  static void addManager(ConnectionManager manager) {
    activeManagers.add(manager);
    Log.warningf("Added ConnectionManager, there are %d active managers", activeManagers.size());
  }

  /**
   * this will check if a connection is local to VM or not
   * this can happen if a client is created within server e.g. we do that in NamespaceMetaStore
   * when this internal client connects to server in the same VM, that connection will be
   * recognized by this call. The way it does that is by going through other ConnectionManager
   * objects created in the same VM and check if thay hold the other end of a given conection,
   *
   * @param serverSideEndpoints
   * @param serverSideManager
   * @return
   */
  private static final boolean isLocalToVM(Pair<IPAndPort, IPAndPort> serverSideEndpoints,
      ConnectionManager serverSideManager) {
    Pair<IPAndPort, IPAndPort> clientSideEndPoints = Pair.of(serverSideEndpoints.getV2(), serverSideEndpoints.getV1());
    for (ConnectionManager manager : activeManagers) {
      if (manager != serverSideManager) {
        if (manager.connectionMap.containsKey(clientSideEndPoints)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Map of Connections where Key is a pair of local and remote IPAndPort in that order
   */
  private final ConcurrentMap<Pair<IPAndPort, IPAndPort>, T> connectionMap = new ConcurrentHashMap<>();

  @Override
  public int disconnectAll(String reason) {
    int disconnected = 0;
    int localToVM = 0;
    for (Map.Entry<Pair<IPAndPort, IPAndPort>, T> entry : connectionMap.entrySet()) {
      if (isLocalToVM(entry.getKey(), this)) {
        localToVM = localToVM + 1;
      } else {
        disconnectConnection(entry.getValue(), reason);
        disconnected = disconnected + 1;
      }
    }
    Log.warningf("%s ConnectionManager disconnected %d and found %d localToVM connections", reason, disconnected,
        localToVM);
    return disconnected;
  }

  void addConnection(T c) {
    IPAndPort local = c.getLocalIPAndPort();
    IPAndPort remote = c.getRemoteIPAndPort();
    Pair<IPAndPort, IPAndPort> endPoints = Pair.of(local, remote);
    connectionMap.put(endPoints, c);
    Log.warning("ConnectionManager added ", c);
  }

  Collection<T> getConnections() {
    return connectionMap.values();
  }

  void disconnectConnection(Connection c, String reason) {
    IPAndPort local = c.getLocalIPAndPort();
    IPAndPort remote = c.getRemoteIPAndPort();
    Pair<IPAndPort, IPAndPort> endPoints = Pair.of(local, remote);
    connectionMap.remove(endPoints);
    c.disconnect();
    Log.warning("ConnectionManager disconnect: ", c + " " + reason);
  }
}

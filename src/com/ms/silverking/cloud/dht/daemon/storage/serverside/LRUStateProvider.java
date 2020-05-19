package com.ms.silverking.cloud.dht.daemon.storage.serverside;

import java.util.Queue;

public interface LRUStateProvider {
  Queue<LRUKeyedInfo> getLRUList();
}
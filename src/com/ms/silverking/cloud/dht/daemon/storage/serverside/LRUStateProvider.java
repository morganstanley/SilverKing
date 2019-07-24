package com.ms.silverking.cloud.dht.daemon.storage.serverside;

import java.util.List;

public interface LRUStateProvider {
    List<LRUKeyedInfo> getLRUList();
}
package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.daemon.storage.convergence.RingState;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.collection.Pair;

public interface PeerStateListener {
    public void peerStateMet(DHTConfiguration dhtConfig, Pair<Long,Long> ringVersionPair, RingState state);
}

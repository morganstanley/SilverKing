package com.ms.silverking.cloud.dht.daemon;

interface RingMapStateListener {
    void globalConvergenceComplete(RingMapState ringMapState);
}

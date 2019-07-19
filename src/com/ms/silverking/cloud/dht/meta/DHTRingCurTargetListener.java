package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.collection.Triple;

public interface DHTRingCurTargetListener {
    public void newCurRingAndVersion(Triple<String,Long,Long> curRingAndVersionPair);
    public void newTargetRingAndVersion(Triple<String,Long,Long> targetRingAndVersionPair);
}

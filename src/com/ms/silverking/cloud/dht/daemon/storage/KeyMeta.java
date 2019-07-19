package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.NavigableMap;
import java.util.TreeMap;

import com.ms.silverking.cloud.dht.common.Version;

/**
 * Contains version-->segment mapping. This is a placeholder implementation.
 * Future will use a disk-based tree.
 */
class KeyMeta {
    private NavigableMap<Version, ReadableWritableSegment>  segments;
    
    KeyMeta() {
        segments = new TreeMap<>();
    }
    
    ReadableWritableSegment getSegment(Version version) {
        return segments.ceilingEntry(version).getValue();
    }
}

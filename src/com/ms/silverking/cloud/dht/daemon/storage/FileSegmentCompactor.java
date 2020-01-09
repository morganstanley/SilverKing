package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.collection.Triple;

public interface FileSegmentCompactor {
    HashedSetMap<DHTKey, Triple<Long, Integer, Long>> compact(File nsDir, int segmentNumber, NamespaceOptions nsOptions,
                                                              EntryRetentionCheck retentionCheck, boolean logCompaction) throws IOException;

    void delete(File nsDir, int segmentNumber) throws IOException;

    int emptyTrashAndCompaction(File nsDir);
}

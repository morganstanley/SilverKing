package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.io.IOException;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.collection.Triple;

public interface FileSystemOps {
    HashedSetMap<DHTKey, Triple<Long, Integer, Long>> compactSegment(File nsDir, int segmentNumber, NamespaceOptions nsOptions,
                                                                     EntryRetentionCheck retentionCheck, boolean logCompaction) throws IOException;

    void deleteSegment(File nsDir, int segmentNumber) throws IOException;

    int emptyTrashAndCompactionSegments(File nsDir);

    FileSystemOps globalDefaultFileSystemOps = new FileSystemOps() {
        @Override
        public HashedSetMap<DHTKey, Triple<Long, Integer, Long>> compactSegment(File nsDir, int segmentNumber, NamespaceOptions nsOptions, EntryRetentionCheck retentionCheck, boolean logCompaction) throws IOException {
            return FileSegmentCompactor.compact(nsDir, segmentNumber, nsOptions, retentionCheck, logCompaction);
        }

        @Override
        public void deleteSegment(File nsDir, int segmentNumber) throws IOException {
            FileSegmentCompactor.delete(nsDir, segmentNumber);
        }

        @Override
        public int emptyTrashAndCompactionSegments(File nsDir) {
            return FileSegmentCompactor.emptyTrashAndCompaction(nsDir);
        }
    };
}

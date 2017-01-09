package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Iterator;

import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.collection.Pair;


interface OffsetList extends Iterable<Integer> {
    static final int    NO_MATCH_FOUND = -1;
    
    void putOffset(long version, int offset, long storageTime);
    int getOffset(VersionConstraint vc, ValidityVerifier validityVerifier);
    int getFirstOffset();
    int getLastOffset();
    long getLatestVersion();
    void displayForDebug();
    Iterator<Integer> iterator();
    Iterator<Long> versionIterator();
    Iterator<Pair<Long,Long>>	versionAndStorageTimeIterator();
    MultiVersionChecksum getMultiVersionChecksum();
	int size();
}

package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Iterator;

import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;


interface OffsetList extends Iterable<Integer> {
    static final int    NO_MATCH_FOUND = -1;
    
    void putOffset(long version, int offset, long storageTime);
    int getOffset(VersionConstraint vc, ValidityVerifier validityVerifier);
    int getFirstOffset();
    int getLastOffset();
    long getLatestVersion();
    void displayForDebug();
    Iterator<Integer> iterator();
    Iterable<Long> versionIterable();
    Iterator<Long> versionIterator();
    Iterable<Pair<Long,Long>>	versionAndStorageTimeIterable();
    Iterator<Pair<Long,Long>>	versionAndStorageTimeIterator();
    Iterable<Triple<Integer,Long,Long>>	offsetVersionAndStorageTimeIterable();
    Iterator<Triple<Integer,Long,Long>>	offsetVersionAndStorageTimeIterator();
    MultiVersionChecksum getMultiVersionChecksum();
	int size();
}

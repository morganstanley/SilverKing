package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.Iterator;

import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.collection.Triple;

public interface OffsetList extends Iterable<Integer> {
  public static final int NO_MATCH_FOUND = -1;

  public void putOffset(long version, int offset, long storageTime);

  public int getOffset(VersionConstraint vc, ValidityVerifier validityVerifier);

  public int getFirstOffset();

  public int getLastOffset();

  public long getLatestVersion();

  public void displayForDebug();

  public Iterator<Integer> iterator();

  public Iterable<Long> versionIterable();

  public Iterator<Long> versionIterator();

  public Iterable<Pair<Long, Long>> versionAndStorageTimeIterable();

  public Iterator<Pair<Long, Long>> versionAndStorageTimeIterator();

  public Iterable<Triple<Integer, Long, Long>> offsetVersionAndStorageTimeIterable();

  public Iterator<Triple<Integer, Long, Long>> offsetVersionAndStorageTimeIterator();

  public MultiVersionChecksum getMultiVersionChecksum();

  public int size();
}

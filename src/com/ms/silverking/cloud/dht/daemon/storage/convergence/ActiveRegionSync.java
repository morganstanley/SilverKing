package com.ms.silverking.cloud.dht.daemon.storage.convergence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.MapMaker;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.InternalRetrievalOptions;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksum;
import com.ms.silverking.cloud.dht.daemon.storage.KeyAndVersionChecksumSegmentNumberComparator;
import com.ms.silverking.cloud.dht.daemon.storage.KeyedOpResultListener;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceStore;
import com.ms.silverking.cloud.dht.daemon.storage.StorageValueAndParameters;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.MessageGroupBase;
import com.ms.silverking.cloud.dht.net.MessageGroupConnection;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeRequestMessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoRetrievalMessageGroup;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.numeric.LongInterval;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.SimpleTimer;
import com.ms.silverking.time.Timer;

public class ActiveRegionSync implements KeyedOpResultListener {
  private final UUIDBase uuid;
  private final NamespaceStore nsStore;
  private final long namespace;
  private final ChecksumTreeRequest ctr;
  private final ChecksumTreeServer checksumTreeServer;
  private final MessageGroupBase mgBase;
  private final Lock completionLock;
  private final Condition completionCV;
  private final Set<UUIDBase> inprocessSyncRetrievalRequests;
  private final Map<UUIDBase, SyncRetrievalRequest> outstandingSyncRetrievalRequests;
  private boolean isComplete;
  private volatile long lastUpdateMillis;
  private volatile boolean checksumTreeProcessed;

  public static boolean debug = false;
  private static final boolean verbose = true;

  private static final long checksumVCMin = Long.MIN_VALUE + 1;
  private static final int timeoutCheckMillis = 1 * 60 * 1000;
  private static final int convergenceRelativeDeadlineMillis = 35 * 60 * 1000;
  private static final long checksumTreeRequestTimeout = 1 * 60 * 1000;
  private static final int retrievalBatchSize = 256;
  private static final byte[] emptyUserData = new byte[0];
  private static final int maxInProcess = 2;

  private static final ConcurrentMap<UUIDBase, ActiveRegionSync> activeRegionSyncs;

  static {
    activeRegionSyncs = new MapMaker().weakValues().makeMap();
  }

  private ActiveRegionSync(NamespaceStore nsStore, long namespace, ChecksumTreeServer checksumTreeServer,
      MessageGroupBase mgBase, ChecksumTreeRequest ctr) {
    uuid = UUIDBase.random();
    this.nsStore = nsStore;
    this.namespace = namespace;
    this.ctr = ctr;
    this.mgBase = mgBase;
    this.checksumTreeServer = checksumTreeServer;
    completionLock = new ReentrantLock();
    completionCV = completionLock.newCondition();
    outstandingSyncRetrievalRequests = new ConcurrentHashMap<>();
    inprocessSyncRetrievalRequests = new ConcurrentSkipListSet<>();
    lastUpdateMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
  }

  public ActiveRegionSync(NamespaceStore nsStore, ChecksumTreeServer checksumTreeServer, MessageGroupBase mgBase,
      ChecksumTreeRequest ctr) {
    this(nsStore, nsStore.getNamespace(), checksumTreeServer, mgBase, ctr);
  }

  // for testing
  public ActiveRegionSync(long namespace, ChecksumTreeServer checksumTreeServer, MessageGroupBase mgBase,
      ChecksumTreeRequest ctr) {
    this(null, namespace, checksumTreeServer, mgBase, ctr);
  }

  public UUIDBase getUUID() {
    return uuid;
  }

  public void startSync() {
    sendChecksumTreeRequest(ctr);
  }

  public ChecksumNode createEmptyChecksumTree(RingRegion region) {
    RegionTreeBuilder rtb;
    ChecksumNode root;

    rtb = new RegionTreeBuilder(region, 3, 1);
    root = rtb.getRoot();
    rtb.freeze();
    return root;
  }

  public void incomingChecksumTree(ConvergencePoint cp, ChecksumNode remoteTree, MessageGroupConnection connection) {
    ChecksumNode localTree;
    MatchResult matchResult;
    Set<KeyAndVersionChecksum> keysToFetch;
    List<KeyAndVersionChecksum> keysToFetchList;
    SyncRetrievalRequest initialSRR;

    lastUpdateMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
    if (verbose) {
      Log.warningAsyncf("incomingChecksumTree %s %s %s estimatedKeys %s", uuid, cp,
          (connection != null ? connection.getRemoteIPAndPort() : "null connection"),
          (remoteTree != null ? Integer.toString(remoteTree.estimatedKeys()) : "null remoteTree"));
    }
    if (remoteTree == null) {
      if (verbose) {
        Log.warningAsyncf("ars null remote tree %s", uuid);
      }
      checksumTreeProcessed = true;
      checkForCompletion();
      return;
    }
    if (checksumTreeServer != null) {
      localTree = checksumTreeServer.getRegionChecksumTree_Local(cp, remoteTree.getRegion(),
          new LongInterval(Long.MIN_VALUE, cp.getDataVersion()));
      if (localTree == null) {
        localTree = createEmptyChecksumTree(remoteTree.getRegion());
      }
    } else {
      if (verbose) {
        Log.warningAsync("checksumTreeServer == null\t%s", uuid);
      }
      localTree = createEmptyChecksumTree(remoteTree.getRegion());
    }
    if (debug) {
      System.out.printf("incomingChecksumTree %x\t%s\t%s\t%s\n", namespace, cp, remoteTree.getRegion(), uuid);
      //System.out.println(remoteTree + "\n\nlocalTree\n" + localTree);
    }
    if (localTree == null) {
      if (verbose) {
        Log.warningAsyncf("ars null local tree %s", uuid);
      }
      checksumTreeProcessed = true;
      checkForCompletion();
      return;
    }
    try {
      matchResult = TreeMatcher.match(localTree, remoteTree);
    } catch (RuntimeException re) {
      System.err.println(localTree);
      System.err.println();
      System.err.println(connection);
      System.err.println(remoteTree);
      throw re;
    }
    if (verbose) {
      Log.warningAsyncf("matchResult %s %s", uuid, matchResult.toSummaryString());
    }
    if (debug) {
      System.out.println(matchResult);
    }
    keysToFetch = new HashSet<>(matchResult.getDestNotInSource().size());
    for (KeyAndVersionChecksum kvc : matchResult.getDestNotInSource()) {
      if (debug) {
        System.out.printf("Adding destNotInSource %s\n", kvc.getKey());
      }
      keysToFetch.add(kvc);
    }
    for (KeyAndVersionChecksum kvc : matchResult.getChecksumMismatch()) {
      if (debug) {
        System.out.printf("Adding checksumMismatch %s\n", kvc.getKey());
      }
      keysToFetch.add(kvc);
    }

    initialSRR = null;
    keysToFetchList = new ArrayList<>(keysToFetch);
    Collections.sort(keysToFetchList, KeyAndVersionChecksumSegmentNumberComparator.descendingSort);
    while (keysToFetchList.size() > 0) {
      Set<DHTKey> batchKeys;
      int batchSize;
      UUIDBase uuid;
      SyncRetrievalRequest srr;

      batchKeys = new ConcurrentSkipListSet<DHTKey>();
      batchSize = 0;
      while (keysToFetchList.size() > 0 && batchSize < retrievalBatchSize) {
        batchKeys.add(keysToFetchList.remove(0).getKey());
        ++batchSize;
      }

      // FUTURE - could consider sending SourceNotInDest
      uuid = UUIDBase.random();
      srr = new SyncRetrievalRequest(uuid, batchKeys, cp.getDataVersion(), connection);
      activeRegionSyncs.put(uuid, this);
      outstandingSyncRetrievalRequests.put(uuid, srr);
      if (initialSRR == null) {
        initialSRR = srr;
      }
      checksumTreeProcessed = true;
    }
    if (debug) {
      System.out.println("no more keysToFetch");
    }
    if (initialSRR != null) {
      sendSyncRetrievalRequest(initialSRR);
    } else {
      if (outstandingSyncRetrievalRequests.size() != 0) {
        throw new RuntimeException("Panic");
      }
    }
    checksumTreeProcessed = true;
    checkForCompletion();
  }

  ////////////////////////////////////////////////////////////////////

  public void incomingSyncRetrievalResponse(MessageGroup message) {
    try {
      List<StorageValueAndParameters> svpList;
      SyncRetrievalRequest srr;

      lastUpdateMillis = SystemTimeUtil.skSystemTimeSource.absTimeMillis();
      srr = outstandingSyncRetrievalRequests.get(message.getUUID());

      if (verbose) {
        Log.warningAsyncf("incoming srr %s %d", message.getUUID(), message.estimatedKeys());
      }
      svpList = new ArrayList<>();
      for (MessageGroupRetrievalResponseEntry entry : message.getRetrievalResponseValueKeyIterator()) {
        StorageValueAndParameters svp;

        if (debug) {
          Log.warningf("%s", entry.toString());
        }
        if (srr != null) {
          srr.outstandingKeys.remove(entry);
        }
        try {
          svp = StorageValueAndParameters.createSVP(entry);
        } catch (RuntimeException re) {
          Log.warningf("Exception processing %s", IPAddrUtil.addrToString(message.getOriginator()));
          throw re;
        }
        if (svp != null) {
          svpList.add(svp);
        }
      }
      if (!svpList.isEmpty()) {
        if (nsStore != null) {
          // FUTURE - support migration of user data
          nsStore.put(svpList, emptyUserData, this);
        }
      }
      if (srr != null && srr.outstandingKeys.isEmpty()) {
        Log.warningAsyncf("ars %s complete-srr %s", uuid, srr.getUUID());
        outstandingSyncRetrievalRequests.remove(srr.getUUID());
        inprocessSyncRetrievalRequests.remove(srr.getUUID());
      }
      checkForCompletion();
    } catch (RuntimeException re) {
      Log.warningf("Exception in ars for ns %x", namespace);
      throw re;
    }
  }

  ////////////////////////////////////////////////////////////////////

  private void sendChecksumTreeRequest(ChecksumTreeRequest ctr) {
    MessageGroup mg;

    mg = new ProtoChecksumTreeRequestMessageGroup(uuid, namespace, ctr.getTargetCP(), ctr.getCurCP(), mgBase.getMyID(),
        ctr.getRegion(), false).toMessageGroup();
    if (verbose || debug) {
      Log.warningAsyncf("%x requestChecksumTree: %s\t%s\t%s\t%s", namespace, ctr.getReplica(), ctr.getRegion(),
          ctr.getTargetCP(), ctr.getCurCP());
    }
    mgBase.send(mg, ctr.getReplica());
    ctr.setSent();
  }

  private static VersionConstraint checksumVersionConstraint(long max) {
    return new VersionConstraint(checksumVCMin, Long.MAX_VALUE, VersionConstraint.Mode.GREATEST);
    // FUTURE - consider max; for now; always greatest
    //return new VersionConstraint(checksumVCMin, max, VersionConstraint.Mode.GREATEST);
  }

  private void sendSyncRetrievalRequest(SyncRetrievalRequest srr) {
    MessageGroup mg;
    RetrievalOptions retrievalOptions;

    Log.warningAsyncf("ars %s send srr %s", uuid, srr.getUUID());
    inprocessSyncRetrievalRequests.add(srr.getUUID());
    retrievalOptions = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE_AND_META_DATA, WaitMode.GET,
        checksumVersionConstraint(srr.dataVersion));
    mg = new ProtoRetrievalMessageGroup(srr.uuid, namespace, new InternalRetrievalOptions(retrievalOptions),
        mgBase.getMyID(), srr.outstandingKeys, convergenceRelativeDeadlineMillis,
        TraceIDProvider.noTraceID).toMessageGroup();
    mgBase.send(mg, srr.connection.getRemoteIPAndPort());
  }

  //KeyedOpResultListener implementation; only used for ignoring results of puts for now
  public void sendResult(DHTKey key, OpResult result) {
  }

  //////////////////////////////////////////////////////////////////////
  // completion check

  private void checkForCompletion() {
    if (checksumTreeProcessed && outstandingSyncRetrievalRequests.isEmpty()) {
      setComplete();
    } else {
      int inprocessSize;

      inprocessSize = inprocessSyncRetrievalRequests.size();
      Log.warningAsyncf("ars progress: %s outstanding %d inprocess %d", uuid, outstandingSyncRetrievalRequests.size(),
          inprocessSize);
      if (inprocessSize < maxInProcess) {
        for (Map.Entry<UUIDBase, SyncRetrievalRequest> e : outstandingSyncRetrievalRequests.entrySet()) {
          if (!inprocessSyncRetrievalRequests.contains(e.getKey())) {
            sendSyncRetrievalRequest(e.getValue());
            break;
          }
        }
      }
    }
  }

  ////////////////////////////////////////////////////////////////////

  private void setComplete() {
    completionLock.lock();
    try {
      Log.warningAsyncf("ars setComplete %s", uuid);
      isComplete = true;
      completionCV.signalAll();
    } finally {
      completionLock.unlock();
    }
  }

  public boolean waitForCompletion(long time, TimeUnit unit) {
    do {
      Timer timer;

      timer = new SimpleTimer(TimeUnit.MILLISECONDS, timeoutCheckMillis);
      completionLock.lock();
      try {
        while (!isComplete) {
          boolean checkTimedOut; // just this check; not the sync

          try {
            if (debug || verbose) {
              Log.warningAsyncf("ars completionCV.await %s", uuid);
            }
            checkTimedOut = !timer.await(completionCV);
            if (debug || verbose) {
              Log.warningAsyncf("ars completionCV.await done %s", uuid);
            }
          } catch (InterruptedException e) {
            checkTimedOut = false;
          }
          if (checkTimedOut) {
            break;
          }
        }
      } finally {
        completionLock.unlock();
      }
    } while (!isComplete && SystemTimeUtil.skSystemTimeSource.absTimeMillis() - lastUpdateMillis < TimeUnit.MILLISECONDS.convert(
        time, unit));
    return isComplete;
  }

  //////////////////////////////////////////////////////////////////////////////////////

  protected static AbsMillisTimeSource absMillisTimeSource;

  public static void setAbsMillisTimeSource(AbsMillisTimeSource _absMillisTimeSource) {
    absMillisTimeSource = _absMillisTimeSource;
  }

  private static class SyncRetrievalRequest {
    final UUIDBase uuid;
    final Set<DHTKey> outstandingKeys;
    final long dataVersion;
    final MessageGroupConnection connection;
    long sendTime;

    SyncRetrievalRequest(UUIDBase uuid, Set<DHTKey> outstandingKeys, long dataVersion,
        MessageGroupConnection connection) {
      this.uuid = uuid;
      this.outstandingKeys = outstandingKeys;
      this.dataVersion = dataVersion;
      this.connection = connection;
    }

    UUIDBase getUUID() {
      return uuid;
    }

    void setSent() {
      sendTime = absMillisTimeSource.absTimeMillis();
    }

    boolean hasTimedOut() {
      return absMillisTimeSource.absTimeMillis() > sendTime + checksumTreeRequestTimeout;
    }

    @Override
    public String toString() {
      return connection.getRemoteIPAndPort() + " " + outstandingKeys.size();
    }
  }

  public static boolean _incomingSyncRetrievalResponse(MessageGroup message) {
    ActiveRegionSync ars;

    ars = activeRegionSyncs.get(message.getUUID());
    if (ars != null) {
      ars.incomingSyncRetrievalResponse(message);
      return true;
    } else {
      Log.info("No ActiveRegionSync for uuid: ", message.getUUID());
      return false;
    }
  }
}

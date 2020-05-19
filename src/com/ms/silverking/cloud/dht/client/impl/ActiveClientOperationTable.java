package com.ms.silverking.cloud.dht.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ms.silverking.cloud.dht.client.OperationState;
import com.ms.silverking.cloud.dht.common.JVMUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.daemon.storage.convergence.ChecksumNode;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.cloud.dht.net.ProtoChecksumTreeMessageGroup;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.log.Log;
import com.ms.silverking.thread.lwt.LWTPool;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;

public class ActiveClientOperationTable {
  private final ActivePutListeners activePutListeners;
  private final ActiveRetrievalListeners activeRetrievalListeners;
  private final ActiveVersionedBasicOperations activeVersionedBasicOperations;
  private final LWTPool lwtPool;
  private final Stopwatch finalizationSW;

  private static final boolean debugTimeouts = false;

  private static final double finalizationLoadThreshold = 0.1;
  private static final double finalizationForceIntervalSeconds = 10.0;

  public ActiveClientOperationTable() {
    activePutListeners = new ActivePutListeners();
    activeRetrievalListeners = new ActiveRetrievalListeners();
    activeVersionedBasicOperations = new ActiveVersionedBasicOperations();
    lwtPool = LWTPoolProvider.defaultConcurrentWorkPool;
    finalizationSW = new SimpleStopwatch();
  }

  public ActivePutListeners getActivePutListeners() {
    return activePutListeners;
  }

  public ActiveRetrievalListeners getActiveRetrievalListeners() {
    return activeRetrievalListeners;
  }

  public ActiveVersionedBasicOperations getActiveVersionedBasicOperations() {
    return activeVersionedBasicOperations;
  }

  public List<AsyncOperationImpl> getActiveAsyncOperations() {
    List<AsyncOperationImpl> activeOps;

    activeOps = new ArrayList<>();
    activeOps.addAll(activeRetrievalListeners.currentRetrievalSet());
    activeOps.addAll(activePutListeners.currentPutSet());
    // activeOps.addAll(currentOpSet()); FIXME - ops not currently supported
    return activeOps;
  }

  // FUTURE - for testing, consider removing
  public void receivedChecksumTree(MessageGroup message) {
    ChecksumNode checksumNode;

    checksumNode = ProtoChecksumTreeMessageGroup.deserialize(message);
    System.out.println(checksumNode);
    activeVersionedBasicOperations.receivedOpResponse(message);
  }

  public <K extends UUIDBase, V> void debugMap(Map<K, V> map, UUIDBase base) {
    for (Map.Entry<K, V> entry : map.entrySet()) {
      System.out.println(
          entry.getKey() + "\t->\t" + entry.getValue() + base.hashCode() + "\t" + entry.getKey().hashCode() + "\t" + entry.getKey().equals(
              base));
    }
  }

  // timeout code
  private static boolean allowFinalization = false;

  public static void disableFinalization() {
    allowFinalization = false;
  }

  public void checkForTimeouts(long curTimeMillis, OpSender opSender, OpSender putSender, OpSender retrievalSender,
      boolean exclusionSetHasChanged) {
        /*
        Set<AsyncRetrievalOperationImpl>   crs;
        Set<AsyncPutOperationImpl>   cps;
        
        crs = activeRetrievalListeners.currentRetrievalSet();
        // temporary debugging
        //System.gc();
        //System.runFinalization();
        System.out.printf("Time %d\tcurrentRetrievalSet %d\t", curTimeMillis, crs.size());
        //for (AsyncRetrievalOperationImpl op : crs) {
        //    op.debugReferences();
        //}
        // temporary debugging
        cps = activePutListeners.currentPutSet();
        System.out.printf("Time %d\tcurrentPutSet %d\n", curTimeMillis, cps.size());
        //checkOpsForTimeouts(curTimeMillis, crs, retrievalSender);
        checkOpsForTimeouts(curTimeMillis, cps, putSender);
         *
         */

    // FUTURE - think about gc and finalization, for now avoid finalization since it
    //          incurs massive lag
    if (allowFinalization && (lwtPool.getLoad().getLoad() < finalizationLoadThreshold || finalizationSW.getSplitSeconds() > finalizationForceIntervalSeconds)) {
      System.gc();
      JVMUtil.getGlobalFinalization().forceFinalization((int) (finalizationForceIntervalSeconds * 1000.0));
      finalizationSW.reset();
    }

    Set<AsyncRetrievalOperationImpl> crs;

    crs = activeRetrievalListeners.currentRetrievalSet();
    //System.out.printf("Time %d\tcurrentRetrievalSet %d\n", curTimeMillis, crs.size());
    checkOpsForTimeouts(curTimeMillis, crs, retrievalSender, exclusionSetHasChanged);

    Set<AsyncPutOperationImpl> cps;

    cps = activePutListeners.currentPutSet();
    checkOpsForTimeouts(curTimeMillis, cps, putSender, exclusionSetHasChanged);
  }

  private void checkOpsForTimeouts(long curTimeMillis, Set<? extends AsyncOperationImpl> ops, OpSender opSender,
      boolean exclusionSetHasChanged) {
    if (debugTimeouts) {
      System.out.println("ActiveClientOperationTable.checkOpsForTimeouts " + ops.size());
    }
    for (AsyncOperationImpl op : ops) {
      checkOpForTimeouts(curTimeMillis, op, opSender, exclusionSetHasChanged);
    }
  }

  private void checkOpForTimeouts(long curTimeMillis, AsyncOperationImpl op, OpSender opSender,
      boolean exclusionSetHasChanged) {
    boolean attemptHasTimedOut;

    attemptHasTimedOut = op.attemptHasTimedOut(curTimeMillis);
    if (attemptHasTimedOut || (exclusionSetHasChanged && op.retryOnExclusionChange(curTimeMillis))) {
      if (op.getState() == OperationState.INCOMPLETE) {
        if (debugTimeouts) {
          if (attemptHasTimedOut) {
            Log.warning("Attempt timed out: ", op);
          } else {
            Log.warning("Retry due to exclusion set change: ", op);
          }
        }
        if (!op.opHasTimedOut(curTimeMillis)) {
          if (debugTimeouts) {
            Log.warning("Resending: ", op);
          } else {
            Log.info("Resending: ", op);
          }
          if (attemptHasTimedOut) {
            // only bump up attempts if the attempt has timed out
            // not if the exclusion set has changed
            op.newAttempt(curTimeMillis);
          }
          opSender.addWork(op);
        } else {
          if (debugTimeouts) {
            Log.warning("Op timed out: ", op);
          }
          Log.info("TIMEOUT: ", op);
          op.setResult(OpResult.TIMEOUT);
        }
      }
    }
  }
}

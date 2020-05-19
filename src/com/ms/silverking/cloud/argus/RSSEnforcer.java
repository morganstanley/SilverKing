package com.ms.silverking.cloud.argus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.log.Log;
import com.ms.silverking.os.linux.proc.ProcReader;
import com.ms.silverking.os.linux.proc.ProcessStatAndOwner;
import com.ms.silverking.thread.ThreadUtil;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.ParseExceptionAction;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/**
 * Enforces limit on RSS size. Sum of all RSS must not exceed physical memory.
 */
public class RSSEnforcer implements SafetyEnforcer {
  private final ProcReader procReader;
  private final List<String> exceptions;
  private final Terminator terminator;
  private final ArgusOptions options;
  private final String userName;
  private final PeerWarningModule udpModule;

  private static final long ONEMB = 1024 * 1024;
  private static final long defaultCandidateMinThreasholdMB = 1 * 1024;
  private static final long defaultFreeMemoryLimitMB = 300;
  private static final long defaultFreeRAMLimitMB = 50;
  private static final long defaultFreeSwapLimitMB = 0;
  private static final long defaultFastCheckThresholdMB = 512;
  private static final long defaultSwapFastCheckThresholdMB = 2048;
  private static final int defaultFastIntervalMillis = 300;
  private static final int defaultSlowIntervalMillis = 10 * 1000;
  private static final String defaultPropExceptions = ".*java.*:.*Argus.*";

  // kill pause is long enough to prevent re-kill for most circumstances,
  //  but we allow the possibility of re-kills
  private static final int killPauseMillis = 20;
  private static final String propCandidateMinThreashold = "candidateMinThresholdMB";
  private static final String propFreeMemoryLimit = "freeMemoryLimitMB";
  private static final String propFreeRAMLimit = "freeRAMLimitMB";
  private static final String propFastCheckThreshold = "fastCheckThresholdMB";
  private static final String propSwapFastCheckThreshold = "swapFastCheckThresholdMB";
  private static final String propFreeSwapLimit = "freeSwapLimitMB";
  private static final String propFastIntervalMillis = "fastIntervalMillis";
  private static final String propSlowIntervalMillis = "slowIntervalMillis";
  private static final String propExceptions = "RSSExceptions";
  private static final String delimiter = ":";

  private final long candidateMinThreashold;
  private final long freeMemoryLimit;
  private final long freeRAMLimit;
  private final long freeSwapLimit;
  private final long fastCheckThreshold;
  private final long swapFastCheckThreshold;
  private final int fastIntervalMillis;
  private final int slowIntervalMillis;

  public RSSEnforcer(PropertiesHelper ph, Terminator terminator, ArgusOptions options, PeerWarningModule udpModule) {
    this.terminator = terminator;
    assert options != null;
    this.options = options;
    this.udpModule = udpModule;
    procReader = new ProcReader();

    candidateMinThreashold = ph.getLong(propCandidateMinThreashold, defaultCandidateMinThreasholdMB,
        ParseExceptionAction.DefaultOnParseException) * ONEMB;
    freeMemoryLimit = ph.getLong(propFreeMemoryLimit, defaultFreeMemoryLimitMB,
        ParseExceptionAction.DefaultOnParseException) * ONEMB;
    freeRAMLimit = ph.getLong(propFreeRAMLimit, defaultFreeRAMLimitMB,
        ParseExceptionAction.DefaultOnParseException) * ONEMB;
    freeSwapLimit = ph.getLong(propFreeSwapLimit, defaultFreeSwapLimitMB,
        ParseExceptionAction.DefaultOnParseException) * ONEMB;
    fastCheckThreshold = ph.getLong(propFastCheckThreshold, defaultFastCheckThresholdMB,
        ParseExceptionAction.DefaultOnParseException) * ONEMB;
    swapFastCheckThreshold = ph.getLong(propSwapFastCheckThreshold, defaultFastCheckThresholdMB,
        ParseExceptionAction.DefaultOnParseException) * ONEMB;
    fastIntervalMillis = ph.getInt(propFastIntervalMillis, defaultFastIntervalMillis,
        ParseExceptionAction.DefaultOnParseException);
    slowIntervalMillis = ph.getInt(propSlowIntervalMillis, defaultSlowIntervalMillis,
        ParseExceptionAction.DefaultOnParseException);
    String[] exs = ph.getString(propExceptions, defaultPropExceptions).split(delimiter);
    exceptions = Arrays.asList(exs);

    userName = PropertiesHelper.systemHelper.getString("user.name", UndefinedAction.ExceptionOnUndefined);
    if (options.singleUser) {
      Log.warning("RSSEnforcer is running in singleUser enforcement mode");
    }
  }

  @Override
  public int enforce() {
    Log.info("Enforcing RSS");
    try {
      long freeBytes;
      long freeSwapBytes;
      long freeRAMBytes;

      freeBytes = procReader.freeMemBytes();
      Log.info("Free bytes: ", freeBytes);
      freeRAMBytes = procReader.freeRamBytes();
      Log.info("Free RAM bytes: ", freeRAMBytes);
      freeSwapBytes = procReader.freeSwapBytes();
      Log.info("Free swap bytes: ", freeSwapBytes);
      if (freeBytes < freeMemoryLimit || freeRAMBytes < freeRAMLimit || (freeRAMBytes < freeMemoryLimit && freeSwapBytes < freeSwapLimit)) {
        terminateUntilFree();
      }
      if (freeBytes < fastCheckThreshold || freeSwapBytes < swapFastCheckThreshold) {
        Log.info("fast1");
        Log.infof("freeBytes          %d", freeBytes);
        Log.infof("fastCheckThreshold %d", fastCheckThreshold);
        Log.infof("freeSwapBytes          %d", freeSwapBytes);
        Log.infof("swapFastCheckThreshold %d", swapFastCheckThreshold);
        return fastIntervalMillis;
      } else {
        Log.infof("System.currentTimeMillis() %d", System.currentTimeMillis());
        Log.infof("Argus.lastPeerWarningMillis %d", Argus.lastPeerWarningMillis);
        Log.infof("System.currentTimeMillis() - Argus.lastPeerWarningMillis %d",
            System.currentTimeMillis() - Argus.lastPeerWarningMillis);
        Log.infof("Argus.peerWarningResponseIntervalMillis %d", Argus.peerWarningResponseIntervalMillis);
        if (System.currentTimeMillis() - Argus.lastPeerWarningMillis < Argus.peerWarningResponseIntervalMillis) {
          Log.info("fast2");
          return fastIntervalMillis;
        } else {
          Log.info("slow1");
          return slowIntervalMillis;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Log.warning(e);
      Log.info("slow2");
      return slowIntervalMillis;
    }
  }

  private void terminateUntilFree() throws IOException {
    long freeBytes;
    long freeSwapBytes;
    long freeRAMBytes;

    Log.info("terminateUntilFree");
    freeBytes = procReader.freeMemBytes();
    freeSwapBytes = procReader.freeSwapBytes();
    freeRAMBytes = procReader.freeRamBytes();
    while (freeBytes < freeMemoryLimit || (freeRAMLimit > 0 && freeRAMBytes < freeRAMLimit) || (freeSwapLimit > 0 && freeRAMBytes < freeMemoryLimit && freeSwapBytes < freeSwapLimit)) {
      terminateLargest(freeBytes < freeMemoryLimit, freeSwapBytes < freeSwapLimit);
      ThreadUtil.sleep(killPauseMillis);
      freeBytes = procReader.freeMemBytes();
      freeSwapBytes = procReader.freeSwapBytes();
    }
  }

  private void terminateLargest(boolean rssExceeded, boolean swapExceeded) throws IOException {
    List<ProcessStatAndOwner> processList;
    ProcessStatAndOwner largest;
    String errorMessage;

    if (udpModule != null) {
      udpModule.warnPeers();
    }
    errorMessage = (rssExceeded ? "RSS memory limit exceeded " : "") + (swapExceeded ?
        "Swap limit exceeded " :
        "") + ": ";
    processList = getCandidateProcessList();
    if (processList.size() > 0) {
      largest = processList.get(processList.size() - 1);
      String msg = new String(
          errorMessage + largest.getOwner() + "\t" + largest.getStat().pid + "\t" + largest.getStat().comm + "\t" + largest.getStat().getRSSBytes());
      Log.warning(msg);
      terminator.terminate(largest.getStat().pid, msg);
    } else {
      Log.warning("terminateLargest processList empty");
    }
  }

  private List<ProcessStatAndOwner> getCandidateProcessList() throws IOException {
    List<ProcessStatAndOwner> processList;
    List<Integer> pidList;
    RSSCandidateComparator candidateComparator;
    Set<String> prioritizedUserPatterns;

    pidList = procReader.filteredActivePIDList(exceptions);
    processList = new ArrayList<ProcessStatAndOwner>();
    for (int pid : pidList) {
      ProcessStatAndOwner candidate;

      candidate = procReader.readStatAndOwner(pid);
      if (candidate != null && candidate.getStat().getRSSBytes() >= candidateMinThreashold) {
        if (!options.singleUser || userName.equals(candidate.getOwner())) {
          processList.add(candidate);
        } else {
          Log.info("Ignoring: ", candidate);
        }
      }
    }

    switch (options.rssCandidateComparisonMode) {
    case USER_PRIORITY_AND_PROCESS_RSS:
      prioritizedUserPatterns = ImmutableSet.copyOf(
          options.prioritizedUserPatterns.split(ArgusOptions.prioritizedUserDelimiter));
      candidateComparator = new UserPriorityAndRSSComparator(prioritizedUserPatterns,
          options.rssPrioritizationThreshold);
      break;
    case USER_PRIORITY_AND_USER_RSS:
      prioritizedUserPatterns = ImmutableSet.copyOf(
          options.prioritizedUserPatterns.split(ArgusOptions.prioritizedUserDelimiter));
      candidateComparator = new UserPriorityAndUserRSSComparator(prioritizedUserPatterns,
          options.rssPrioritizationThreshold, processList);
      break;
    default:
      throw new RuntimeException("Unexpected rssCandidateComparisonMode");
    }

    Collections.sort(processList, candidateComparator);
    return processList;
  }
}

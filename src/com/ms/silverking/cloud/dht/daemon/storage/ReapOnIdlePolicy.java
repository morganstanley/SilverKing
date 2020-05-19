package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.logging.Level;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.text.ObjectDefParser2;

public class ReapOnIdlePolicy extends BaseReapPolicy<ReapOnIdleState> {
  private final boolean reapOnStartup;
  private final boolean reapOnIdle;
  private final long minIdleIntervalMillis;
  private final long minFullReapIntervalMillis;
  private final long minPutDelta;
  private final int idleReapPauseMillis;
  private final int reapIntervalMillis;
  private final int batchLimit;
  private final ReapPolicy.EmptyTrashMode emptyTrashMode;

  private static final boolean defaultReapOnStartup = true;
  private static final boolean defaultReapOnIdle = true;
  private static final long defaultMinIdleIntervalMillis = 4 * 60 * 1000;
  private static final long defaultMinFullReapIntervalMillis = 4 * 60 * 60 * 1000;
  private static final long defaultMinPutDelta = 10;
  private static final int defaultIdleReapPauseMillis = 5;
  private static final int defaultReapIntervalMillis = 5 * 60 * 1000;
  private static final int defaultBatchLimit = 1000;
  private static final ReapPolicy.EmptyTrashMode defaultEmptyTrashMode = EmptyTrashMode.EveryFullReap;

  private static final boolean debug = false;

  private static final ReapOnIdlePolicy template = new ReapOnIdlePolicy();

  static {
    ObjectDefParser2.addParser(template);
  }

  public ReapOnIdlePolicy(boolean verboseReap, boolean verboseReapPhase, boolean verboseSegmentDeletion,
      boolean reapOnStartup, boolean reapOnIdle, long minIdleIntervalMillis, long minFullReapIntervalMillis,
      long minPutDelta, int idleReapPauseMillis, int reapIntervalMillis, int defaultBatchLimit,
      EmptyTrashMode emptyTrashMode) {
    super(verboseReap, verboseReapPhase, verboseSegmentDeletion);
    this.reapOnStartup = reapOnStartup;
    this.reapOnIdle = reapOnIdle;
    this.minIdleIntervalMillis = minIdleIntervalMillis;
    this.minFullReapIntervalMillis = minFullReapIntervalMillis;
    this.minPutDelta = minPutDelta;
    this.idleReapPauseMillis = idleReapPauseMillis;
    this.reapIntervalMillis = reapIntervalMillis;
    this.emptyTrashMode = emptyTrashMode;
    this.batchLimit = defaultBatchLimit;
  }

  public ReapOnIdlePolicy() {
    this(defaultVerboseReap, defaultVerboseReapPhase, defaultVerboseSegmentDeletion, defaultReapOnStartup,
        defaultReapOnIdle, defaultMinIdleIntervalMillis, defaultMinFullReapIntervalMillis, defaultMinPutDelta,
        defaultIdleReapPauseMillis, defaultReapIntervalMillis, defaultBatchLimit, defaultEmptyTrashMode);
  }

  public ReapOnIdlePolicy reapOnStartup(boolean reapOnStartup) {
    return new ReapOnIdlePolicy(verboseReap, verboseReapPhase, verboseSegmentDeletionAndCompaction, reapOnStartup,
        reapOnIdle, minIdleIntervalMillis, minFullReapIntervalMillis, minPutDelta, idleReapPauseMillis,
        reapIntervalMillis, batchLimit, emptyTrashMode);
  }

  public ReapOnIdlePolicy reapOnIdle(boolean reapOnIdle) {
    return new ReapOnIdlePolicy(verboseReap, verboseReapPhase, verboseSegmentDeletionAndCompaction, reapOnStartup,
        reapOnIdle, minIdleIntervalMillis, minFullReapIntervalMillis, minPutDelta, idleReapPauseMillis,
        reapIntervalMillis, batchLimit, emptyTrashMode);
  }

  @Override
  public EmptyTrashMode getEmptyTrashMode() {
    return emptyTrashMode;
  }

  @Override
  public ReapOnIdleState createInitialState() {
    return new ReapOnIdleState();
  }

  @Override
  public boolean reapAllowed(ReapOnIdleState state, NamespaceStore nsStore, ReapPhase reapPhase, boolean isStartup) {
    if (isStartup) {
      return reapOnStartup;
    } else {
      if (!reapOnIdle) {
        return false;
      } else {
        NamespaceMetrics nsMetrics;

        nsMetrics = nsStore.getNamespaceMetrics();
        if (nsMetrics.getTotalPuts() - state.getPutsAsOfLastFullReap() < minPutDelta) {
          if (Log.levelMet(Level.INFO) || debug) {
            Log.warningf("minPutDelta not met for ns %x", nsStore.getNamespace());
          }
          return false;
        } else {
          long timeMillis;

          timeMillis = SystemTimeUtil.timerDrivenTimeSource.absTimeMillis();
          if (timeMillis - state.getLastFullReapMillis() < minFullReapIntervalMillis) {
            if (Log.levelMet(Level.INFO) || debug) {
              Log.warningf("minFullReapIntervalMillis not met for ns %x", nsStore.getNamespace());
            }
            return false;
          } else {
            if (timeMillis - nsMetrics.getLastActivityMillis() < minIdleIntervalMillis) {
              if (Log.levelMet(Level.INFO) || debug) {
                Log.warningf("minIdleIntervalMillis not met for ns %x", nsStore.getNamespace());
              }
              return false;
            } else {
              return true;
            }
          }
        }
      }
    }
  }

  @Override
  public int getIdleReapPauseMillis() {
    return idleReapPauseMillis;
  }

  @Override
  public boolean supportsLiveReap() {
    return reapOnIdle;
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  @Override
  public int getReapIntervalMillis() {
    return reapIntervalMillis;
  }

  @Override
  public int getBatchLimit(ReapPhase reapPhase) {
    return batchLimit;
  }
}

package com.ms.silverking.cloud.argus;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimerTask;

import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.LogMode;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;
import com.ms.silverking.util.SafeTimer;
import com.ms.silverking.util.SafeTimerTask;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 * Argus ensures that servers are not damaged by user processes
 * by enforcing a set of safety constraints. These constraints
 * are enforced by concrete SafetyEnforcer implementations.
 */
public class Argus implements PeerWarningListener {
  private static final String propKillEnabled = "killEnabled";
  private static final String defaultKillEnabled = "false";
  private static final String propEventsLogDir = "eventsLogDir";
  private static final String defaultEventsLogDir = "/tmp";
  private static final String udpPort = "udpPort";
  private static final String warningFile = "warningFile";
  private static final String warningFileIntervalSeconds = "warningFileIntervalSeconds";
  private static final String peerWarningResponseIntervalSeconds = "peerWarningResponseIntervalSeconds";
  private final List<SafetyEnforcer> enforcers;
  private final SafeTimer timer;

  private enum Test {RSS, DiskUsage}

  ;
  private boolean killEnabled;
  private Terminator terminator;
  private Terminator.KillType killtype;
  private volatile SafeTimerTask nextRSSTask;
  private final RSSEnforcer rssEnforcer;
  private PeerWarningModule peerWarningModule;

  public static long lastPeerWarningMillis;
  public static long peerWarningResponseIntervalMillis;

  private static final int defaultWarningFileIntervalSeconds = 60;
  private static final long defaultPeerWarningResponseIntervalSeconds = 10 * 60;

  public Argus(ArgusOptions options) throws IOException {
    enforcers = new ArrayList<>();

    killtype = Terminator.KillType.valueOf(options.terminatorType);
    if (options.DiskUsageEnforcer != null) {
      enforcers.add(initEnforcer(Test.DiskUsage, options.DiskUsageEnforcer, options));
    }
    enforcers.add(initEnforcer(Test.RSS, options.RSSEnforcer, options));
    rssEnforcer = (RSSEnforcer) enforcers.get(enforcers.size() - 1);

    timer = new SafeTimer();
  }

  private SafetyEnforcer initEnforcer(Test test, String testSpec, ArgusOptions options) throws IOException {
    Properties prop = new Properties();
    try {
      FileInputStream fis = new FileInputStream(testSpec);
      prop.load(fis);
      fis.close();
    } catch (Exception e) {
      Log.warning(e);
    }
    PropertiesHelper ph;
    int port;
    String wf;
    int wfi;

    ph = new PropertiesHelper(prop, LogMode.UndefinedAndExceptions);

    peerWarningResponseIntervalMillis = ph.getLong(peerWarningResponseIntervalSeconds,
        defaultPeerWarningResponseIntervalSeconds) * 1000;

    port = ph.getInt(udpPort, 0);
    wf = ph.getString(warningFile, (String) null);
    if (wf != null) {
      wfi = ph.getInt(warningFileIntervalSeconds, defaultWarningFileIntervalSeconds);
    } else {
      wfi = defaultWarningFileIntervalSeconds;
    }

    peerWarningModule = new PeerWarningModule(port, this, wf != null ? new File(wf) : null, wfi);

    if (terminator == null) {
      String eventsLogDir;
      String customTerminatorDef;

      eventsLogDir = ph.getString(propEventsLogDir, defaultEventsLogDir);
      if (eventsLogDir == null) {
        throw new RuntimeException(propEventsLogDir + " is not specified in Properties file " + testSpec);
      }
      String loggerFileName = eventsLogDir + "/" + InetAddress.getLocalHost().getHostName();
      killEnabled = Boolean.parseBoolean(ph.getString(propKillEnabled, defaultKillEnabled));
      terminator = new Terminator(killEnabled ? Terminator.Mode.Armed : Terminator.Mode.LogOnly, loggerFileName,
          killtype, options.minKillIntervalSeconds * 1000);
      Log.warning("Argus terminator is running with mode " + terminator.getMode().name() + " and termination type: ",
          killtype);

      customTerminatorDef = ph.getString(Terminator.KillType.CustomTerminator.toString(),
          UndefinedAction.ZeroOnUndefined);
      if (customTerminatorDef != null) {
        Terminator.addKillCommand(Terminator.KillType.CustomTerminator.toString(), customTerminatorDef);
      }
    }

    switch (test) {
    case DiskUsage:
      return new DiskUsageEnforcer(ph, terminator);
    case RSS:
      Log.warning("RSSCandidateComparisonMode: ", options.rssCandidateComparisonMode);
      return new RSSEnforcer(ph, terminator, options, peerWarningModule);
    default:
      throw new RuntimeException("Unimplemented test: " + testSpec);
    }
  }

  public void enforce() {
    for (SafetyEnforcer enforcer : enforcers) {
      timer.schedule(new ArgusTask(enforcer), 0);
    }
  }

  /**
   * @param args cmd-line args
   */
  public static void main(String[] args) {
    try {
      //Log.setLevelAll();
      //Log.setPrintStreams(System.out);

      Log.warning("Argus is starting");

      ArgusOptions options = new ArgusOptions();
      CmdLineParser parser = new CmdLineParser(options);
      try {
        parser.parseArgument(args);
      } catch (CmdLineException cle) {
        System.err.println(cle.getMessage());
        parser.printUsage(System.err);
        return;
      }
      new Argus(options).enforce();
    } catch (Exception e) {
      Log.logErrorWarning(e);
    }
  }

  // This is currently only set up for RSS
  @Override
  public void peerWarning() {
    lastPeerWarningMillis = System.currentTimeMillis();
    if (nextRSSTask != null) {
      nextRSSTask.cancel();
      ArgusTask next;

      Log.info("Argus.receivedPeerWarning()");
      next = new ArgusTask(rssEnforcer);
      timer.schedule(next, 0);
    }
  }

  private class ArgusTask extends TimerTask {
    private final SafetyEnforcer enforcer;

    ArgusTask(SafetyEnforcer enforcer) {
      this.enforcer = enforcer;
    }

    @Override
    public void run() {
      int delayMillis;
      ArgusTask next;

      Log.info(this);
      delayMillis = enforcer.enforce();
      next = new ArgusTask(enforcer);
      if (enforcer instanceof RSSEnforcer) {
        nextRSSTask = new SafeTimerTask(next);
      }
      timer.schedule(next, delayMillis);
    }
  }
}

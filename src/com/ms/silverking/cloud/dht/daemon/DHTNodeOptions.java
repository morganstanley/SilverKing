package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.log.Log;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.ms.silverking.cloud.dht.daemon.storage.ReapOnIdlePolicy;
import com.ms.silverking.cloud.dht.daemon.storage.ReapPolicy;
import com.ms.silverking.text.ObjectDefParser2;

public class DHTNodeOptions {
  public static final int defaultInactiveNodeTimeoutSeconds = Integer.MAX_VALUE;

  @Option(name = "-n", usage = "dhtName", required = true)
  public String dhtName;

  @Option(name = "-z", usage = "zkConfig", required = true)
  public String zkConfig;

  @Option(name = "-into", usage = "inactiveNodeTimeoutSeconds", required = false)
  public int inactiveNodeTimeoutSeconds = defaultInactiveNodeTimeoutSeconds;

  @Option(name = "-reapPolicy", usage = "reapPolicy", required = false)
  String reapPolicy = ObjectDefParser2.toClassAndDefString(new ReapOnIdlePolicy());

  public ReapPolicy getReapPolicy() {
    return (ReapPolicy) ObjectDefParser2.parse(reapPolicy, ReapPolicy.class.getPackage());
  }

  @Option(name = "-daemonIP", usage = "daemonIP", required = false)
  String daemonIP;

  @Option(name = "-port", usage = "Port to use for this node instead of the one declared in DHTConfig. IP Aliasing " +
      "must be used to allow other nodes to connect.")
  public int daemonPortOverride = DHTConstants.noPortOverride;

  public static DHTNodeOptions initialize(String[] args) {
    DHTNodeOptions options;
    CmdLineParser parser;

    options = new DHTNodeOptions();
    parser = new CmdLineParser(options);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException cle) {
      Log.logErrorWarning(cle);
      System.err.println(cle.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }
    if (options.daemonIP != null && options.daemonPortOverride != DHTConstants.noPortOverride) {
      Log.warning("Only one of daemonIP and daemonPortOverride may be set");
      parser.printUsage(System.err);
      System.exit(-1);
    }
    return options;
  }
}

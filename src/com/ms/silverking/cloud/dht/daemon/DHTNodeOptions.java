package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.daemon.storage.ReapOnIdlePolicy;
import com.ms.silverking.cloud.dht.daemon.storage.ReapPolicy;
import com.ms.silverking.text.ObjectDefParser2;
import org.kohsuke.args4j.Option;

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
}

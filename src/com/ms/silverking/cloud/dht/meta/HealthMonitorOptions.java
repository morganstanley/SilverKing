package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.meta.ExclusionZKBase;
import org.kohsuke.args4j.Option;

public class HealthMonitorOptions {
  static final int NO_DOCTOR = 0;

  @Option(name = "-g", usage = "GridConfig", required = true)
  public String gridConfig;
  @Option(name = "-c", usage = "convictionLimits", required = true)
  public String convictionLimits;
  @Option(name = "-w", usage = "convictionWarningThresholds", required = false)
  public String convictionWarningThresholds;
  @Option(name = "-i", usage = "intervalSeconds", required = false)
  public int watchIntervalSeconds = 15;
  @Option(name = "-G", usage = "guiltThreshold", required = false)
  public int guiltThreshold = 3;
  @Option(name = "-dri", usage = "doctorRoundIntervalSeconds", required = false)
  public int doctorRoundIntervalSeconds = NO_DOCTOR;
  @Option(name = "-forceUnsafe", usage = "forceInclusionOfUnsafeExcludedServers", required = false)
  boolean forceInclusionOfUnsafeExcludedServers = false;
  @Option(name = "-dto", usage = "doctorNodeStartupTimeoutSeconds", required = false)
  public int doctorNodeStartupTimeoutSeconds = 5 * 60;
  @Option(name = "-da", usage = "disableAddition", required = false)
  public boolean disableAddition;
  @Option(name = "-m", usage = "minUpdateIntervalSeconds", required = false)
  public int minUpdateIntervalSeconds = 10;
  @Option(name = "-retainedExclusionSets", usage = "retainedExclusionSets", required = false)
  public int retainedExclusionSets = ExclusionZKBase.noRetainedVersionsLimit;

  public boolean doctorRequested() {
    return doctorRoundIntervalSeconds != NO_DOCTOR;
  }
}

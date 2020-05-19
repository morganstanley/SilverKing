package com.ms.silverking.cloud.toporing;

import org.kohsuke.args4j.Option;

public class DependencyWatcherOptions {
  DependencyWatcherOptions() {
  }

  @Option(name = "-g", usage = "gridConfig", required = true)
  String gridConfig;

  @Option(name = "-i", usage = "watchIntervalSeconds", required = false)
  int watchIntervalSeconds = 15;

  @Option(name = "-f", usage = "ignoreFeasibility", required = false)
  boolean ignoreFeasibility = false;

  @Option(name = "-s", usage = "ignoreSource", required = false)
  boolean ignoreSource = false;

  @Option(name = "-I", usage = "ignoreInstanceExclusions", required = false)
  boolean ignoreInstanceExclusions = false;

  @Option(name = "-x", usage = "exitAfterBuild", required = false)
  boolean exitAfterBuild = false;

  @Option(name = "-c", usage = "consecutiveUpdateGuardSeconds", required = false)
  int consecutiveUpdateGuardSeconds = 60;
}

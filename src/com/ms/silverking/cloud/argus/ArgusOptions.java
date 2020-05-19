package com.ms.silverking.cloud.argus;

import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class ArgusOptions {
  ArgusOptions() {
    //this is left empty
  }

  @Option(name = "-s", usage = "singleUser", required = false)
  boolean singleUser = false;

  // deprecated - FUTURE - remove
  @Option(name = "-p", usage = "productionUsers", required = false)
  String productionUsers;

  @Option(name = "-d", usage = "DiskUsageEnforcer", required = false)
  String DiskUsageEnforcer;

  @Option(name = "-r", usage = "RSSEnforcer", required = true)
  String RSSEnforcer;

  @Option(name = "-m", usage = "RSSCandidateComparisonMode", required = false)
  RSSCandidateComparisonMode rssCandidateComparisonMode = RSSCandidateComparisonMode.USER_PRIORITY_AND_USER_RSS;

  @Option(name = "-t", usage = "TerminatorType")
  String terminatorType = "KillTerminator";  // default is KillTerminator

  @Option(name = "-u", usage = "prioritizedUserPatterns")
  String prioritizedUserPatterns = "";

  @Option(name = "-T", usage = "rssPrioritizationThreshold")
  int rssPrioritizationThreshold = defaultRSSPrioritizationThreshold;

  @Option(name = "-i", usage = "minKillIntervalSeconds", required = false)
  long minKillIntervalSeconds = 0;

  static final String prioritizedUserDelimiter = ":";
  static final int defaultRSSPrioritizationThreshold = 1 * 1024 * 1024;

  @Argument
  List<String> arguments = new ArrayList<String>();
}

package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.impl.NamespaceCreator;
import com.ms.silverking.cloud.dht.client.impl.SimpleNamespaceCreator;
import com.ms.silverking.collection.CollectionUtil;
import org.kohsuke.args4j.Option;

public class DHTRingMasterOptions {
  @Option(name = "-g", usage = "GridConfig", required = true)
  public String gridConfig;
  @Option(name = "-i", usage = "intervalSeconds", required = false)
  public int watchIntervalSeconds = 10;
  @Option(name = "-m", usage = "Mode", required = false)
  public Mode mode = Mode.Manual;
  @Option(name = "-pcm", usage = "PassiveConvergenceMode", required = false)
  public PassiveConvergenceMode passiveConvergenceMode = PassiveConvergenceMode.FullSync_FailOnFailure;
  @Option(name = "-ignoreNamespacesHex", usage = "ignoreNamespacesHex", required = false)
  public String ignoreNamespacesHex;

  public Set<Long> getIgnoredNamespaces() {
    if (ignoreNamespacesHex == null) {
      return ImmutableSet.of();
    } else {
      NamespaceCreator nsc;
      HashSet<Long> ignoredNamespaces;

      nsc = new SimpleNamespaceCreator();
      ignoredNamespaces = new HashSet<>();
      for (String ns : CollectionUtil.parseSet(ignoreNamespacesHex, ",")) {
        ignoredNamespaces.add(Long.decode(ns));
      }
      return ignoredNamespaces;
    }
  }
}

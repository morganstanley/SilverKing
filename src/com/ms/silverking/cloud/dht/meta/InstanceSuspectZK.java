package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.meta.SuspectZKBase;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;

/**
 * Instance-specific SuspectSet
 */
public class InstanceSuspectZK extends SuspectZKBase<MetaPaths> {
  public InstanceSuspectZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getInstanceSuspectsPath());
  }
}

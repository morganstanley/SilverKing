package com.ms.silverking.cloud.dht.meta;

import com.ms.silverking.cloud.meta.ExclusionZKBase;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;

/**
 * Instance-specific ExclusionSet
 */
public class InstanceExclusionZK extends ExclusionZKBase<MetaPaths> {
  public InstanceExclusionZK(MetaClient mc) throws KeeperException {
    super(mc, mc.getMetaPaths().getInstanceExclusionsPath());
  }
}

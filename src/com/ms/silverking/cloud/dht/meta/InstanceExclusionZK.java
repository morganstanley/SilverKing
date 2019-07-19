package com.ms.silverking.cloud.dht.meta;

import org.apache.zookeeper.KeeperException;

import com.ms.silverking.cloud.meta.ExclusionZKBase;

/**
 * Instance-specific ExclusionSet
 */
public class InstanceExclusionZK extends ExclusionZKBase<MetaPaths> {
    public InstanceExclusionZK(MetaClient mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getInstanceExclusionsPath());
    }
}

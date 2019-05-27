package com.ms.silverking.cloud.meta;

import org.apache.zookeeper.KeeperException;

/**
 * Cloud-level ExclusionSet
 */
public class ExclusionZK extends ExclusionZKBase<MetaPaths> {
    public ExclusionZK(MetaClient mc) throws KeeperException {
        super(mc, mc.getMetaPaths().getExclusionsPath());
    }
}

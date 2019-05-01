package com.ms.silverking.cloud.zookeeper;

import org.apache.zookeeper.data.ACL;

import java.util.List;

// TODO: remove this when curator is used to replace ZooKeeperExtended
public interface SKAclProvider {
    public List<ACL> getDefaultAcl();
    public List<ACL> getAclForPath(String path);
}

package com.ms.silverking.cloud.zookeeper;

import com.ms.silverking.text.ObjectDefParser2;
import org.apache.zookeeper.data.ACL;

import java.util.List;

// TODO: remove this when curator is used to replace ZooKeeperExtended
public interface SKAclProvider {
  public List<ACL> getDefaultAcl();

  public List<ACL> getAclForPath(String path);

  // TODO: Remove this workaround
  public static SKAclProvider parse(String skDef) {
    return ObjectDefParser2.parse(skDef, SKAclProvider.class.getPackage());
  }
}

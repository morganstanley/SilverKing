package com.ms.silverking.cloud.zookeeper;

import org.apache.curator.framework.CuratorFramework;

public interface CuratorWithBasePath {
  String getResolvedPath(String path);

  CuratorFramework getCurator();
}

package com.ms.silverking.cloud.dht.trace;

public interface TracerContext {
  String getEnv();

  String getClusterName();

  public String getHost();

  public int getPort();
}

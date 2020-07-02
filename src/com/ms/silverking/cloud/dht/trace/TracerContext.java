package com.ms.silverking.cloud.dht.trace;

public interface TracerContext {
  String getEnv();

  public String getHost();

  public int getPort();
}

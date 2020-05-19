package com.ms.silverking.cloud.dht.daemon.storage;

public enum ReapMode {
  None, OnStartup, OnIdle, OnStartupAndIdle;

  public boolean reapsOnStartup() {
    return this == OnStartup || this == OnStartupAndIdle;
  }

  public boolean reapsOnIdle() {
    return this == OnIdle || this == OnStartupAndIdle;
  }
}

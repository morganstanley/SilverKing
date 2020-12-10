package com.ms.silverking.cloud.dht;

public enum ForwardingMode {
  DO_NOT_FORWARD, FORWARD, ALL;

  public boolean forwards() {
    return this != DO_NOT_FORWARD;
  }
}

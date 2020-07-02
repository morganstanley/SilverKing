package com.ms.silverking.cloud.dht.trace;

public final class EmptyTracerContext implements TracerContext {

  public String getEnv() { return ""; }

  public String getHost() { return ""; }

  public int getPort() { return -1; }
}
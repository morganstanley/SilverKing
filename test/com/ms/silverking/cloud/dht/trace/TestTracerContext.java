package com.ms.silverking.cloud.dht.trace;

public class TestTracerContext implements TracerContext {
  public static TracerContext instance = new TestTracerContext();

  private TestTracerContext() {}

  public String getEnv() { return "test"; }

  public String getClusterName() { return "[unknown]"; }

  public String getHost() { return "localhost"; }

  public int getPort() { return -1; }
}

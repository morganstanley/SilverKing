package com.ms.silverking.cloud.dht.trace;

import com.google.common.base.Preconditions;

public class TracerFactory {
  private static Tracer singletonTracerInstance = null;

  public static void setTracer(Tracer instance) {
    Preconditions.checkNotNull(instance);
    singletonTracerInstance = instance;
  }

  // This method will be called at launch time
  public static void ensureTracerInitialized() {
    Preconditions.checkNotNull(singletonTracerInstance,
        "Tracer hasn't been set in TracerFactory in a server who has trace feature enabled");
  }

  public static Tracer getTracer() {
    return singletonTracerInstance;
  }

  public static boolean isInitialized() {
    return singletonTracerInstance != null;
  }
}
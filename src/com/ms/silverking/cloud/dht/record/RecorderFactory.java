package com.ms.silverking.cloud.dht.record;

import com.google.common.base.Preconditions;

public class RecorderFactory {
  private static Recorder singletonRecorderInstance = null;

  public static void setRecorder(Recorder instance) {
    Preconditions.checkNotNull(instance);
    singletonRecorderInstance = instance;
  }

  // This method will be called at launch time
  public static void ensureInitialized() {
    Preconditions.checkNotNull(singletonRecorderInstance,
        "Recorder hasn't been set in RecorderFactory in a server who has record feature enabled");
  }

  public static Recorder getRecorder() {
    return singletonRecorderInstance;
  }

  public static boolean isInitialized() {
    return singletonRecorderInstance != null;
  }

  public static void clear() {
    singletonRecorderInstance = null;
  }

}

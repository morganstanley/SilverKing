package com.ms.silverking.cloud.dht.daemon;

import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.JVMUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.memory.JVMMemoryObserver;
import com.ms.silverking.util.memory.JVMMonitor;

/**
 * Watches over JVM heap usage.
 */
public class MemoryManager implements JVMMemoryObserver {
  private final JVMMonitor monitor;

  private static final int jvmMonitorMinIntervalMillis = PropertiesHelper.systemHelper.getInt(
      DHTConstants.jvmMonitorMinIntervalMillisProperty, 10 * 1000);
  private static final int jvmMonitorMaxIntervalMillis = PropertiesHelper.systemHelper.getInt(
      DHTConstants.jvmMonitorMaxIntervalMillisProperty, 30 * 1000);
  //private static final int jvmMonitorMaxIntervalMillis = 15 * 60 * 1000;
  //private static final int jvmFinalizationIntervalMillis = 15 * 60 * 1000;
  private static final int jvmFinalizationIntervalMillis = Integer.MAX_VALUE;
  private static final double jvmMonitorLowMemoryThresholdMB = 50;

  public MemoryManager() {
    monitor = new JVMMonitor(jvmMonitorMinIntervalMillis, jvmMonitorMaxIntervalMillis, jvmFinalizationIntervalMillis,
        true, jvmMonitorLowMemoryThresholdMB, JVMUtil.getGlobalFinalization());
    monitor.addMemoryObserver(this);
  }

  @Override
  public void jvmMemoryLow(boolean isLow) {
    Log.warning("Memory low!");
  }

  @Override
  public void jvmMemoryStatus(long bytesFree) {
  }

  public JVMMonitor getJVMMonitor() {
    return monitor;
  }
}

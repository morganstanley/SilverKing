package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.cloud.dht.common.SystemTimeUtil;
import com.ms.silverking.log.Log;
import com.ms.silverking.util.PropertiesHelper;

public class TimeBasedSegmentRollOver {
  public static final String enableProp = "com.ms.silverking.cloud.dht.daemon.storage.enableTimeBasedSegmentRollOver";
  public static final String timeThresholdMillisProp = "com.ms.silverking.cloud.dht.daemon.storage.timeBasedSegmentRollOverThresholdMillis";

  public static final boolean isEnabled = PropertiesHelper.systemHelper.getBoolean(enableProp, false);
  private static final long timeThresholdMillis = PropertiesHelper.systemHelper.getLong(timeThresholdMillisProp, -1);

  public static boolean needRollOverOnPut(WritableSegmentBase currSegment) {
    if (timeThresholdMillis < 0) {
      Log.warning("TimeBasedSegmentRollOver's timeThresholdMillis is negative - it may hasn't been set via property:" + timeThresholdMillisProp );
      return false;
    } else {
      return SystemTimeUtil.skSystemTimeSource.absTimeMillis()- currSegment.getSegmentCreationMillis() > timeThresholdMillis;
    }
  }
}

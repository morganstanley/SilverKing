package com.ms.silverking.test;

import com.google.caliper.Benchmark;
import com.google.caliper.runner.CaliperMain;
import com.ms.silverking.thread.ThreadUtil;

public class CaliperTest {

  @Benchmark
  public void timeSleep(int reps) {
    for (int i = 0; i < reps; i++) {
      ThreadUtil.sleep(1);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    CaliperMain.main(CaliperTest.class, args);
  }
}

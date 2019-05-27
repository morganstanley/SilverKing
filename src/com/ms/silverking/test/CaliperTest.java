package com.ms.silverking.test;

import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.ms.silverking.thread.ThreadUtil;

public class CaliperTest extends SimpleBenchmark {
    public void timeSleep(int reps) {
        for (int i = 0; i < reps; i++) {
            ThreadUtil.sleep(1);
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        Runner.main(CaliperTest.class, args);
    }
}

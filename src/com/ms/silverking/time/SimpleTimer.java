package com.ms.silverking.time;

import java.util.concurrent.TimeUnit;

public class SimpleTimer extends StopwatchBasedTimer {
    public SimpleTimer(TimeUnit unit, long limit) {
        super(new SimpleStopwatch(), unit, limit);
    }
}

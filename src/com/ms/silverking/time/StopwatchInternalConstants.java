package com.ms.silverking.time;

import java.math.BigDecimal;

class StopwatchInternalConstants {
    static final double     nanosPerSecond = 1000000000.0;
    static final BigDecimal nanosPerSecondBD = new BigDecimal(nanosPerSecond);
    static final long       millisPerSecond = 1000;
    static final long       nanosPerMilli = 1000000;
}

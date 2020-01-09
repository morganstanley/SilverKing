package com.ms.silverking.cloud.dht.daemon.storage;

import com.ms.silverking.time.AbsNanosTimeSource;

public class DummyTimeSource implements AbsNanosTimeSource {
    private long time = 0L;

    public void setTime(long newTime) {
        time = newTime;
    }

    @Override
    public long getNanosOriginTime() {
        return 0L;
    }

    @Override
    public long absTimeNanos() {
        return time;
    }

    @Override
    public long relNanosRemaining(long absDeadlineNanos) {
        return 0L;
    }
}

package com.ms.silverking.net.async;

public interface QueueingConnectionLimitListener {
    public void queueAboveLimit();
    public void queueBelowLimit();
}

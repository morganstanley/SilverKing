package com.ms.silverking.net.async;

import com.ms.silverking.id.UUIDBase;

public interface MultipleConnectionQueueLengthListener {
    public void queueLength(UUIDBase uuid, int queueLength, Connection maxQueuedConnection);
}

package com.ms.silverking.cloud.dht.record;

import com.ms.silverking.cloud.dht.net.MessageGroup;

public interface Recorder {
  void record(MessageGroup message, byte[] traceId);
}

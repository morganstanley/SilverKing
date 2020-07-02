package com.ms.silverking.cloud.dht.trace;

import com.ms.silverking.text.ObjectDefParser2;

/**
 * This TraceIdProvider impl is used by default in SilverKing
 */
public class NoTraceIDProvider implements TraceIDProvider {
  private static final NoTraceIDProvider template = new NoTraceIDProvider();

  static {
    ObjectDefParser2.addParser(template);
  }

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof NoTraceIDProvider);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  @Override
  public byte[] traceID() {
    return TraceIDProvider.noTraceID;
  }
}

package com.ms.silverking.cloud.dht.trace;

import com.ms.silverking.text.ObjectDefParser2;

public class EmptyTraceIDProvider implements TraceIDProvider {
  private static final EmptyTraceIDProvider template = new EmptyTraceIDProvider();

  static {
    ObjectDefParser2.addParser(template);
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof EmptyTraceIDProvider);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  @Override
  public byte[] traceID() {
    return TraceIDProvider.emptyTraceID;
  }
}

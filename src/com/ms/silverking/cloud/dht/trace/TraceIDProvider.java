package com.ms.silverking.cloud.dht.trace;

import com.ms.silverking.cloud.dht.common.MessageType;

/**
 * Provide customizable trace information for each MessageGroup
 */
public interface TraceIDProvider {
  byte[] noTraceID = null;
  byte[] emptyTraceID = new byte[0];

  TraceIDProvider emptyTraceIDProvider = new EmptyTraceIDProvider();
  TraceIDProvider noTraceIDProvider = new NoTraceIDProvider();

  default boolean isEnabled() {
    return true;
  }

  byte[] traceID();

  static boolean isValidTraceID(byte[] maybeTraceID) {
    return maybeTraceID != noTraceID;
  }

  static boolean hasTraceID(MessageType msgType) {
    switch (msgType) {
    case PUT_TRACE:
    case RETRIEVE_TRACE:
    case PUT_RESPONSE_TRACE:
    case PUT_UPDATE_TRACE:
    case RETRIEVE_RESPONSE_TRACE:
      return true;
    default:
      return false;
    }
  }
}

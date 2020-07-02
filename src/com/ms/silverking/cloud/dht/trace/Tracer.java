package com.ms.silverking.cloud.dht.trace;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.net.IPAndPort;

public interface Tracer extends HasTracerContext {
  /* the API naming conversion for its prefix:
   *  onBoth: this callback func could be called on both proxy and final replica
   *  onProxy: this callback func is only called on proxy
   *  onLocal: this callback fun is only called on final replica
   */

  // Phase 0: scheduling of receiving and handling request
  void onBothReceiveRequest(byte[] traceID);

  void onBothHandleRetrievalRequest(byte[] traceID);

  // Phase 1: if proxy is going to forward message
  byte[] issueForwardTraceID(byte[] maybeTraceID, IPAndPort replica, MessageType msgType,
      byte[] originator); // Tracer will issue a new traceID for final replica

  // Phase 2: get value from storage and put them in queue
  void onLocalHandleRetrievalRequest(byte[] traceID);

  void onLocalEnqueueRetrievalResult(byte[] traceID);

  // Phase 3: Take the results from queue and send them to destination (can be client or proxy)
  // TODO: consider distinguish if result is sent from proxy or final replica
  void onBothDequeueAndAsyncSendRetrievalResult(byte[] traceID);

  // Phase 4: Proxy check the complete status of result
  void onProxyHandleRetrievalResultComplete(byte[] traceID);

  void onProxyHandleRetrievalResultIncomplete(byte[] traceID);

  void onLocalReap(long elapsedTime);

  void onForceReap(long elapsedTime);

  void onQueueLengthInterval(int queueLength);
}

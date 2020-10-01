package com.ms.silverking.cloud.dht.trace;

import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.common.MessageType;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.log.Log;
import com.ms.silverking.net.IPAndPort;

public class TestTracer<T> implements Tracer {
  @FunctionalInterface
  public interface TraceIDTester<R> {
    R testTraceID(byte[] traceID);
  }

  private TraceIDTester<T> tester;
  private List<T> testResults;

  public TestTracer(TraceIDTester<T> tester) {
    this.tester = tester;
    testResults = new ArrayList<>();
  }

  public TracerContext getContext() {
    return TestTracerContext.instance;
  }

  synchronized public void clearResults() {
    testResults.clear();
  }

  synchronized public void addResult(T res) {
    testResults.add(res);
  }

  synchronized public List<T> getTestResults() {
    return new ArrayList<>(testResults);
  }

  @Override
  public void onBothReceiveRequest(byte[] traceID) {
    Log.info("onBothReceiveRequest: " + Thread.currentThread().getName());
    addResult(tester.testTraceID(traceID));
  }

  @Override
  public void onBothHandleRetrievalRequest(byte[] traceID) {
    Log.info("onBothHandleRetrievalRequest: " + Thread.currentThread().getName());
    addResult(tester.testTraceID(traceID));
  }

  @Override
  public byte[] issueForwardTraceID(byte[] maybeTraceID, IPAndPort replica, MessageType msgType, byte[] originator) {
    Log.info("issueForwardTraceID: " + replica + "," + msgType + "," + new SimpleValueCreator(
        originator) + "," + Thread.currentThread().getName());
    addResult(tester.testTraceID(maybeTraceID));
    return maybeTraceID;
  }

  @Override
  public void onLocalHandleRetrievalRequest(byte[] traceID) {
    Log.info("onLocalHandleRetrievalRequest: " + Thread.currentThread().getName());
    addResult(tester.testTraceID(traceID));
  }

  @Override
  public void onLocalEnqueueRetrievalResult(byte[] traceID) {
    Log.info("onLocalEnqueueRetrievalResult: " + Thread.currentThread().getName());
    addResult(tester.testTraceID(traceID));
  }

  @Override
  public void onBothDequeueAndAsyncSendRetrievalResult(byte[] traceID) {
    Log.info("onBothDequeueAndAsyncSendRetrievalResult: " + Thread.currentThread().getName());
    addResult(tester.testTraceID(traceID));
  }

  @Override
  public void onProxyHandleRetrievalResultComplete(byte[] traceID) {
    Log.info("onProxyHandleRetrievalResultComplete: " + Thread.currentThread().getName());
    addResult(tester.testTraceID(traceID));
  }

  @Override
  public void onProxyHandleRetrievalResultIncomplete(byte[] traceID) {
    Log.info("onProxyHandleRetrievalResultIncomplete: " + Thread.currentThread().getName());
    addResult(tester.testTraceID(traceID));
  }

  @Override
  public void onLocalReap(long elapsed) {
    Log.info("onLocalReap " + getContext().getHost() + ": " + Thread.currentThread().getName());
  }

  @Override
  public void onForceReap(long elapsed) {
    Log.info("onForceReap " + getContext().getHost() + ": " + Thread.currentThread().getName());
  }

  @Override
  public void onQueueLengthInterval(int queueLength) {
    Log.info("onQueueLengthInterval " + getContext().getHost() + ": " + Thread.currentThread().getName());
  }
}
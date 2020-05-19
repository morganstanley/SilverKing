package com.ms.silverking.cloud.dht.client.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;

abstract class FragmentedValue<R> implements ActiveKeyedOperationResultListener<R> {
  protected final DHTKey[] keys;
  protected final AtomicInteger resultsReceived;
  protected final Map<DHTKey, R> results;
  protected final DHTKey relayKey;
  protected ActiveKeyedOperationResultListener<R> parent;
  protected final int trackRelayKeyCompletion;

  private static final boolean debug = false;

  FragmentedValue(DHTKey[] keys, DHTKey relayKey, ActiveKeyedOperationResultListener<R> parent,
      boolean trackRelayKeyCompletion) {
    this.keys = keys;
    this.parent = parent;
    resultsReceived = new AtomicInteger();
    this.relayKey = relayKey;
    this.trackRelayKeyCompletion = trackRelayKeyCompletion ? 1 : 0;
    results = new ConcurrentHashMap<>(keys.length + this.trackRelayKeyCompletion); // subkeys + the relay key
  }

  @Override
  public void resultReceived(DHTKey key, R result) {
    R prevResult;

    if (debug) {
      if (result instanceof MessageGroupRetrievalResponseEntry) {
        MessageGroupRetrievalResponseEntry _result;

        _result = (MessageGroupRetrievalResponseEntry) result;
        System.out.printf("FragmentedValue.resultReceived key: %s\tresult: %s\tvalue %s\n", key, result,
            ((MessageGroupRetrievalResponseEntry) result).getValue());
      } else {
        System.out.printf("FragmentedValue.resultReceived key: %s\tresult: %s\tresultsReceived %d\n", key, result,
            resultsReceived.get());
      }
    }
    prevResult = results.put(key, result);
    if (prevResult != null) {
      // FIXME
    } else {
      resultsReceived.incrementAndGet();
    }
    if (resultsReceived.get() >= keys.length + trackRelayKeyCompletion) { // all keys + relay key
      checkForCompletion();
    }
  }

  protected abstract void checkForCompletion();
}

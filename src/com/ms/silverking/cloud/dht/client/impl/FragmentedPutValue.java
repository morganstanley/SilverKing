package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;

class FragmentedPutValue extends FragmentedValue<OpResult> {
  FragmentedPutValue(DHTKey[] keys, DHTKey relayKey, ActiveKeyedOperationResultListener<OpResult> parent) {
    super(keys, relayKey, parent, true);
  }

  private OpResult getResult(DHTKey key) {
    OpResult result;

    result = results.get(key);
    return result == null ? OpResult.INCOMPLETE : result;
  }

  @Override
  protected void checkForCompletion() {
    OpResult result;

    result = OpResult.SUCCEEDED;
    for (DHTKey key : keys) {
      if (getResult(key) != OpResult.SUCCEEDED) {
        result = getResult(key);
      }
    }
    if (getResult(relayKey) != OpResult.SUCCEEDED) {
      result = getResult(relayKey);
    }
    parent.resultReceived(relayKey, result);
  }
}

package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.collection.HashedListMap;
import com.ms.silverking.log.Log;

class KeyedOpResultMultiplexor implements KeyedOpResultListener {
  private final HashedListMap<DHTKey, KeyedOpResultListener> listeners;

  KeyedOpResultMultiplexor() {
    listeners = new HashedListMap<>();
  }

  void addListener(DHTKey key, KeyedOpResultListener listener) {
    listeners.addValue(key, listener);
  }

  @Override
  public void sendResult(DHTKey key, OpResult result) {
    List<KeyedOpResultListener> keyListeners;

    keyListeners = listeners.getList(key);
    if (keyListeners != null) {
      for (KeyedOpResultListener keyListener : keyListeners) {
        keyListener.sendResult(key, result);
      }
    } else {
      Log.warningf("KeyedOpResultMultiplexor. No listeners for %s", KeyUtil.keyToString(key));
    }
  }
}

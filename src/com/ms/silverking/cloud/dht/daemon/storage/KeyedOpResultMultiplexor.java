package com.ms.silverking.cloud.dht.daemon.storage;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.KeyUtil;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.log.Log;

class KeyedOpResultMultiplexor implements KeyedOpResultListener {
	private final Map<DHTKey, KeyedOpResultListener>	listeners;
	
	KeyedOpResultMultiplexor() {
		listeners = new HashMap<>();
	}
	
	void addListener(DHTKey key, KeyedOpResultListener listener) {
		listeners.put(key, listener);
	}

	@Override
	public void sendResult(DHTKey key, OpResult result) {
		KeyedOpResultListener	listener;
		
		listener = listeners.get(key);
		if (listener != null) {
			listener.sendResult(key, result);
		} else {
			Log.warningf("KeyedOpResultMultiplexor. No listener for %s", KeyUtil.keyToString(key));
		}
	}
}
